/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		scheduler.c
 *
 * author: Pavel Stehule, 2015, Czech Republic,
 * licenced under BSD licence
 *
 *-------------------------------------------------------------------------
 */
#include "scheduler.h"

#include "utils/timestamp.h"

/*
 * Todo:
 *      --- 1. fix SQL execution
 *      --- 2. security label and configuration load
 *      3. Throtling and worker state machine
 *      4. Notify event listeners
 *      6. split to files and cleaning
 *      7. tests
 *      8. publishing
 */

#define MAX_JOBS				100
#define MAX_DATABASES				100

static JBSCH_ScheduledJobData jobs_configurations[MAX_JOBS];
static NameData active_databases[MAX_DATABASES];

static int n_job_configurations = 0;
static int n_active_databases = 0;

static int n_processed_databases = 0;

/* related to shared memory CCC */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static bool configuration_is_done = false;

static char current_database[NAMEDATALEN];


/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static int loops = 0;

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static void prepare_config_reading(char *dbname);

static void
scheduler_sighup(SIGNAL_ARGS)
{
	int	save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * scheduler_sigterm
 *
 * SIGTERM handler
 */
static void
scheduler_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Prepare shared space for communication from any client to scheduler process.
 */
static void
scheduler_shmem_startup(void)
{
	bool	found;
	volatile JBSCH_ConfigurationChangeChannel cfgchange_ch;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	cfgchange_ch = (JBSCH_ConfigurationChangeChannel) ShmemInitStruct("JBSCH_CCHCH",
					    sizeof(JBSCH_ConfigurationChangeChannelData), &found);

	if (!found)
	{
		cfgchange_ch->lock_tranche_id = LWLockNewTrancheId();
		SpinLockInit(&cfgchange_ch->mutex);
		cfgchange_ch->hold_data = false;
	}

	JBSCH_Shm_ConfigurationChangeChannel = cfgchange_ch;

	LWLockRelease(AddinShmemInitLock);
}

#define LEAVE_WHEN_ERROR_OR_QUIT(res, name)		\
if (res == SHM_MQ_DETACHED) \
{ \
	elog(LOG, name ": parent died"); \
	break; \
} \
if (res != SHM_MQ_SUCCESS) \
{ \
	elog(LOG, name ": cannot to get data"); \
	break; \
} \
if (strcmp(received_data, "\\q") == 0) \
{ \
	break; \
} 


/*
 * Execute command specified by job_cmd field.
 *
 */
static bool
SQLExecJob(JBSCH_ScheduledJob jobcfg)
{
	int ret;
	StringInfoData		string;
	bool		result = false;
	TupleDesc		tupdesc;
	HeapTuple		tuple;
	char			*job_cmd;
	MemoryContext		oldMemoryContext;
	int fnum_job_cmd;
	int fnum_job_timeout;
	Interval		*job_timeout;
	int			job_timeout_ms;
	bool			isnull;

	oldMemoryContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&string);
	appendStringInfo(&string, "SELECT * FROM job_schedule WHERE id = %d", jobcfg->id);

	ret = SPI_execute(string.data, true, 0);

	pfree(string.data);

	if (ret != SPI_OK_SELECT)
	{
		elog(LOG, "job executor: cannot to run query to read configuration");
		proc_exit(1);
	}

	if (SPI_processed != 1)
	{
		elog(LOG, "job executor: unexpected result of query to job_schedule (%d rows)", SPI_processed);
		proc_exit(1);
	}

	tupdesc = SPI_tuptable->tupdesc;
	tuple = SPI_tuptable->vals[0];

	fnum_job_cmd = SPI_fnumber(tupdesc, "job_cmd");
	job_cmd = SPI_getvalue(tuple, tupdesc, fnum_job_cmd);
	job_cmd = MemoryContextStrdup(oldMemoryContext, job_cmd);

	fnum_job_timeout = SPI_fnumber(tupdesc, "job_timeout");

	job_timeout = DatumGetIntervalP(SPI_getbinval(tuple, tupdesc, fnum_job_timeout, &isnull));
	job_timeout_ms = jbsch_to_timeoffset(job_timeout) * 1000;

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	if (*job_cmd != '\0')
		result = jbsch_ExecuteSQL(job_cmd, job_timeout_ms);
	else
		elog(LOG, "job executor: job_cmd is empty");

	pfree(job_cmd);

	return result;
}

/*
 * SQL executor
 *
 * Waiting for command \\sqlexec, read params from toc, do lookup to pg_jobschedule,
 * execute SQL statement and inform by backcall parent process.
 *
 */
static void
sqlexec_worker_handler(Datum main_arg)
{
	dsm_segment		*segment;
	JBSCH_DatabaseWorker		self;
	int	res;

	void		*received_data;
	Size		received_bytes;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "database configuration reader");
	segment = dsm_attach(DatumGetInt32(main_arg));
	if (segment == NULL)
		elog(ERROR, "unable map dynamic memory segment");

	self = jbsch_SetupDatabaseWorker(segment);

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGTERM, scheduler_sigterm);
	set_latch_on_sigusr1 = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	while (!got_sigterm)
	{
		/*
		 * With nowait=false it can fall only when parent is detached - finished without
		 * explicit QUIT command
		 */
		res = shm_mq_receive(self->in_mq_handle, &received_bytes, &received_data, false);
		LEAVE_WHEN_ERROR_OR_QUIT(res, "job executor");

		if (strcmp(received_data, "\\execsqljob") == 0)
		{
			JBSCH_ScheduledJob shm_jobcfg = (JBSCH_ScheduledJob) shm_toc_lookup(self->toc, 5);
			char		*msg_success = "\\success";
			char		*msg_error = "\\error";

			if (SQLExecJob(shm_jobcfg))
				shm_mq_send(self->out_mq_handle, strlen(msg_success) + 1, msg_success, false);
			else
				shm_mq_send(self->out_mq_handle, strlen(msg_error) + 1, msg_error, false);
		}
	}

	dsm_detach(segment);

	proc_exit(0);
}

static void
sqlexec_worker_receive_trigger(JBSCH_DatabaseWorker dbworker, void *data, Size bytes, void *received_data)
{
	char *cmd = (char *) received_data;

	if (strcmp(cmd, "\\error") == 0)
		elog(LOG, "scheduler: some wrong");

	jbsch_SendQuitDbworker(dbworker);

	/* ToDo: some tasks can wait to other tasks are finished */
	SetLatch(&MyProc->procLatch);
}

static void
sqlexec_worker_event_trigger(JBSCH_DatabaseWorker dbworker, void *data, JBSCH_DatabaseWorkerEvent event)
{
	char *msg_execsqljob = "\\execsqljob";

	if (event == JBSCH_DBW_EVENT_TRIGGER_STARTED)
		shm_mq_send(dbworker->out_mq_handle, strlen(msg_execsqljob) + 1, msg_execsqljob, true);
}

static void
sqlexec_prepare_worker(int jobcfg_id)
{
	JBSCH_ScheduledJob jobcfg = NULL;
	int	i;

	for (i = 0; i < n_job_configurations; i++)
	{
		if (jobs_configurations[i].id == jobcfg_id)
		{
			jobcfg = &jobs_configurations[i];
			break;
		}
	}

	if (jobcfg != NULL)
	{
		if (!jobcfg->suspended)
		{
			if (!jobcfg->suspended)
			{

				JBSCH_DatabaseWorker sqlexec;
				JBSCH_ScheduledJob shm_jobcfg;

				sqlexec = jbsch_NewDatabaseWorker("job executor", NameStr(jobcfg->dbname), NameStr(jobcfg->job_user),
									 sizeof(JBSCH_ScheduledJob) + 4000, 10, 1024,
								    JBSCH_DBW_MQ_BOTH_DIRECTION,
											    BGW_NEVER_RESTART,
										    &sqlexec_worker_handler,
										    0);

				/* when process is initialized, prepare space for configuration */
				shm_jobcfg = (JBSCH_ScheduledJob) shm_toc_allocate(sqlexec->toc, (sizeof(JBSCH_ScheduledJobData)));
				memcpy(shm_jobcfg, jobcfg, sizeof(JBSCH_ScheduledJobData));
				shm_toc_insert(sqlexec->toc, 5, (void *) shm_jobcfg);

				jbsch_DBWorkersPoolPush(sqlexec, sqlexec_worker_receive_trigger, sqlexec_worker_event_trigger, NULL);
			}
		}
		else
			elog(LOG, "scheduler: ignore configuration - suspended");
	}
	else
		elog(LOG, "scheduler: ignore configuration - invalid id");
}


/*
 * After start reads table job_schedule and send content to parent. When it is started, then
 * read table job_schedule and waiting for getcfg command. Later one by one sending configurations
 * to parent - any configuration is confirmed by back command \\data. Whan all configurations are
 * on parent, \\nodata is send.
 */
static void
config_worker_handler(Datum main_arg)
{
	dsm_segment		*segment;
	JBSCH_DatabaseWorker		self;
	JBSCH_ScheduledJob		jobcfg;

	void		*received_data;
	Size		received_bytes;
	int	res;

	int	ret;
	int	processed = 0;

	const char *msg_data = "\\data";
	const char *msg_nodata = "\\nodata";

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "database configuration reader");
	segment = dsm_attach(DatumGetInt32(main_arg));
	if (segment == NULL)
		elog(ERROR, "unable map dynamic memory segment");

	self = jbsch_SetupDatabaseWorker(segment);

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGTERM, scheduler_sigterm);
	set_latch_on_sigusr1 = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute("SELECT * FROM job_schedule WHERE NOT suspended", true, 0);
	if (ret != SPI_OK_SELECT)
	{
		elog(LOG, "config reader: cannot to run query to read configuration");
		proc_exit(1);
	}

	jobcfg = (JBSCH_ScheduledJob) shm_toc_allocate(self->toc, (sizeof(JBSCH_ScheduledJobData)));
	shm_toc_insert(self->toc, 5, (void *) jobcfg);

	while (!got_sigterm)
	{
		/*
		 * With nowait=false it can fall only when parent is detached - finished without
		 * explicit QUIT command
		 */
		res = shm_mq_receive(self->in_mq_handle, &received_bytes, &received_data, false);
		LEAVE_WHEN_ERROR_OR_QUIT(res, "config reader");

		if (strcmp(received_data, "\\getcfg") == 0)
		{
			if (processed < SPI_processed)
			{
				jbsch_SetScheduledJob(jobcfg, SPI_tuptable->vals[processed++], SPI_tuptable->tupdesc);
				shm_mq_send(self->out_mq_handle, strlen(msg_data) + 1, msg_data, false);
			}
			else
			{
				shm_mq_send(self->out_mq_handle, strlen(msg_nodata) + 1, msg_nodata, false);
			}
		}
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	dsm_detach(segment);

	proc_exit(0);
}


/*
 * stored received configuration. When received \\nodata, close config worker and try
 * to move to next database. When there are not any other database, finish.
 */
static void
config_worker_receive_trigger(JBSCH_DatabaseWorker dbworker, void *data, Size bytes, void *received_data)
{
	char *msg_getcfg = "\\getcfg";
	char *cmd = (char *) received_data;
	JBSCH_ScheduledJob		jobcfg;

	if (strcmp(cmd, "\\data") == 0)
	{
		if (n_job_configurations < MAX_JOBS)
		{
			jobcfg = (JBSCH_ScheduledJob) shm_toc_lookup(dbworker->toc, 5);


			if (!jobcfg->suspended)
			{

				memcpy(&jobs_configurations[n_job_configurations], jobcfg, sizeof(JBSCH_ScheduledJobData));

				StrNCpy(NameStr(jobs_configurations[n_job_configurations++].dbname),
							    current_database,
							    NAMEDATALEN);
			}

			shm_mq_send(dbworker->out_mq_handle, strlen(msg_getcfg) + 1, msg_getcfg, true);
		}
		else
		{
			elog(LOG, "scheduler: cannot to store more job configurations");
			jbsch_SendQuitDbworker(dbworker);
		}
	}
	else if (strcmp(cmd, "\\nodata") == 0)
	{
		jbsch_SendQuitDbworker(dbworker);
		if (n_processed_databases < n_active_databases)
			prepare_config_reading(NameStr(active_databases[n_processed_databases++]));
		else
		{
			elog(LOG, "scheduler: configuration done (%d configurations).", n_job_configurations);
			configuration_is_done = true;
			SetLatch(&MyProc->procLatch);
		}

	}
}

/*
 * Ensure to configuration reading when config worker is ready.
 */
static void
config_worker_event_trigger(JBSCH_DatabaseWorker dbworker, void *data, JBSCH_DatabaseWorkerEvent event)
{
	char *msg_getcfg = "\\getcfg";

	if (event == JBSCH_DBW_EVENT_TRIGGER_STARTED)
		shm_mq_send(dbworker->out_mq_handle, strlen(msg_getcfg) + 1, msg_getcfg, true);
}

/*
 * Prepare config reader worker for specified database.
 */
static void
prepare_config_reading(char *dbname)
{
	JBSCH_DatabaseWorker reader;

	strcpy(current_database, dbname);

	reader = jbsch_NewDatabaseWorker("config reader", dbname, NULL,
						 sizeof(JBSCH_ScheduledJob) + 4000, 10, 1024,
							JBSCH_DBW_MQ_BOTH_DIRECTION,
									    BGW_NEVER_RESTART,
									    &config_worker_handler,
									    0);

	jbsch_DBWorkersPoolPush(reader, config_worker_receive_trigger, config_worker_event_trigger, NULL);
}




/*
 *
 * DBWorkerCommandInfo(tag, toc_key, is_protected, mutex, varsize)
 * 
 * PREPARE_INFO(sqlexec, FINISH)
 * struct (command_id, data_id
 *
 * Database Worker Context ...
 *     dsm_segment
 *     resourceOwner
 *
 *  jbsch_InitDBWorkerContext(&context);
 *
 *  while (jbsch_DBWorkerWaitForCommand(Context, &cmdinfo))
 *  {
 *      switch (jbsch_GetCommandTag(cmdinfo))
 *      {
 *            case EXECSQL:
 *                  // protected ~ protected agains a rewrite of unread data 
 *                  jobcfg = JBSCH_GETCOMMAND_PROTECTED_SHMDATA(cmdinfo, JBSCH_JobSchedule);
                             JBSCH_GETCOMMAND_SHMDATA(...)
 *      }
 
 *      ... if (!jbsch_DBWorkerSendInfo(Context, INFO(sqlexec, DONE))
 *               jbsch_DBWorkerSendInfo(Context, INFO_WITH_DATA(FINISH, &var, vartype, TOC_KEY_CMDXXX))
 *              break;
 *  }
 *
 *  jbsch_DestroyDBWorkerContext(&context);
 *  proc_exit(0);
 *
 * Database Worker Controller
 *
 *      EventTrigger(ControllerInfo, void *data, EVENT)
 *          jbsch_SendCommand(COMMAND[_WITH_DATA( ..)]
            jbsch_
 *          JBSCH_GETINFO_SHMDATA(info)
 *   newctr = jbsch_NewDBWorkerController(...,  JBSCH_COMMAND(..))
 *
 * DeterminateSleepTime(...)
 */

static void
database_worker_handler(Datum main_arg)
{
	dsm_segment		*segment;
	JBSCH_DatabaseWorker		self;

	void		*received_data;
	Size		received_bytes;
	int	res;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "database worker");
	segment = dsm_attach(DatumGetInt32(main_arg));
	if (segment == NULL)
		elog(ERROR, "unable map dynamic memory segment");

	self = jbsch_SetupDatabaseWorker(segment);

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGTERM, scheduler_sigterm);
	set_latch_on_sigusr1 = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	while (!got_sigterm)
	{
		/*
		 * With nowait=false it can fall only when parent is detached - finished without
		 * explicit QUIT command
		 */
		res = shm_mq_receive(self->in_mq_handle, &received_bytes, &received_data, false);
		LEAVE_WHEN_ERROR_OR_QUIT(res, "worker");

		if (strcmp(received_data, "\\inc") == 0)
		{
			char		outbuf[20];

			elog(LOG, "worker: received increment command");
			snprintf(outbuf, 20, "%d", ++loops);
			shm_mq_send(self->out_mq_handle, strlen(outbuf) + 1, outbuf, false);
		}

		if (strcmp(received_data, "\\short_sql") == 0)
		{
			char *sqlstr = jbsch_FetchShortSQLCmd(self);

			jbsch_ExecuteSQL(sqlstr, -1);
			pfree(sqlstr);
		}
	}

	dsm_detach(segment);

	proc_exit(0);
}

static void
simple_dbw_event_trigger(JBSCH_DatabaseWorker dbworker, void *data, JBSCH_DatabaseWorkerEvent event)
{
	char *msgdata = "\\inc";
	char *msgdata2 = "\\short_sql";
	JBSCH_DatabaseWorkerPoolEntry			pe;

	char *sqlstr = "do $$begin raise log '<<<AHOJ>>>'; end $$";

	switch (event)
	{
		case JBSCH_DBW_EVENT_TRIGGER_STARTED:
			pe = (JBSCH_DatabaseWorkerPoolEntry) dbworker->pool_entry;
			jbsch_SetDbWorkerTimeoutRepeat(dbworker, *((int *) pe->data));
			loops = 0;
			break;
		case JBSCH_DBW_EVENT_TRIGGER_STOPPED:
			break;
		case JBSCH_DBW_EVENT_TRIGGER_DETACHED:
			break;

		/* resend inc command every sec */
		case JBSCH_DBW_EVENT_TRIGGER_TIME:
			jbsch_SetShortSQLCmd(dbworker, sqlstr);
			shm_mq_send(dbworker->out_mq_handle, strlen(msgdata2) + 1, msgdata2, true);
			shm_mq_send(dbworker->out_mq_handle, strlen(msgdata) + 1, msgdata, true);
			break;
	}
}

static void
simple_dbw_receive_trigger(JBSCH_DatabaseWorker dbworker, void *data, Size bytes, void *received_data)
{
	int receive_loop;
	TimestampTz now;
	char *now_str;
	char *msgbuf = "\\q";

	now = GetCurrentTimestamp();
	now_str = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(now)));

	receive_loop = atoi(received_data);

	elog(LOG, "scheduler: received %s at %s from %s", (char *) received_data, now_str, dbworker->name);

	pfree(now_str);

	/* send quit after 10 iteration */
	if (receive_loop >= 10)
	{
		shm_mq_send(dbworker->out_mq_handle, strlen(msgbuf) + 1, msgbuf, true);
		((JBSCH_DatabaseWorkerPoolEntry) dbworker->pool_entry)->sent_quit = true;
	}
}


static void
main_worker_handler(Datum main_arg)
{
	volatile JBSCH_ConfigurationChangeChannel cfgchange_ch = JBSCH_Shm_ConfigurationChangeChannel;

	JBSCH_DatabaseWorker		dbworker;
	JBSCH_DatabaseWorker		dbworker2;
	ResourceOwner		proResourceOwner;
	TimestampTz			next_job_start;
	bool				next_job_start_is_valid = false;
	int rc;

	int	t1 = 1000;
	int	t2 = 3000;

	/* Connect to database, necessary to get PGPROC */
	/* attention - it reset CurrentResourceOwner to NULL */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	proResourceOwner = ResourceOwnerCreate(NULL, "database scheduler");

	CurrentResourceOwner = proResourceOwner;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	PG_TRY();
	{
		int	ret;
		int	i;
		bool		isnull;

		ret = SPI_execute("SELECT datname"
		                  "    FROM pg_shseclabel l"
		                  "         JOIN pg_database d"
		                  "         ON d.oid = l.objoid"
		                  "   WHERE l.classoid = 'pg_database'::regclass"
		                  "     AND l.provider = 'jobscheduler'"
		                  "     AND l.label = 'active'", true, 0);

		if (ret != SPI_OK_SELECT)
		{
			elog(LOG, "scheduler: cannot to take list of active databases");
			proc_exit(1);
		}

		for (i = 0; i < SPI_processed; i++)
		{
			Name datname;

			if (i >= MAX_DATABASES)
			{
				elog(LOG, "scheduler: buffer of active databases is full, stop filling");
				break;
			}

			datname = DatumGetName(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
			StrNCpy(NameStr(active_databases[n_active_databases++]), NameStr(*datname), NAMEDATALEN);

			elog(LOG, "scheduler: active database: %s", NameStr(active_databases[n_active_databases - 1]));
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		EmitErrorReport();
		AbortCurrentTransaction();
		pgstat_report_activity(STATE_IDLE, NULL);
		FlushErrorState();

		elog(LOG, "some wrong when reading list of active databases");
		proc_exit(0);

	}
	PG_END_TRY();

	CurrentResourceOwner = proResourceOwner;

	if (n_processed_databases < n_active_databases)
	{
		prepare_config_reading(NameStr(active_databases[n_processed_databases++]));
		elog(LOG, "scheduler: configuration reading started");
	}

	dbworker = jbsch_NewDatabaseWorker("worker one", NULL, NULL,
						 4000, 10, 1024,
							JBSCH_DBW_MQ_BOTH_DIRECTION,
									    BGW_NEVER_RESTART,
									    &database_worker_handler,
									    JBSCH_DBWORKER_SHORT_SQLCMD);

	dbworker2 = jbsch_NewDatabaseWorker("worker two", NULL, NULL,
						 4000, 10, 1024,
							JBSCH_DBW_MQ_BOTH_DIRECTION,
									    BGW_NEVER_RESTART,
									    &database_worker_handler,
									    JBSCH_DBWORKER_SHORT_SQLCMD);

	//jbsch_DBWorkersPoolPush(dbworker, simple_dbw_receive_trigger, simple_dbw_event_trigger, &t1);
	//jbsch_DBWorkersPoolPush(dbworker2, simple_dbw_receive_trigger, simple_dbw_event_trigger, &t2);

	cfgchange_ch->scheduler_pid = MyProcPid;

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGTERM, scheduler_sigterm);
	pqsignal(SIGHUP, scheduler_sighup);
	set_latch_on_sigusr1 = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	while (!got_sigterm)
	{
		if (got_sighup)
		{
			JBSCH_ScheduledJobData		jobcfg_data;

			got_sighup = false;

			SpinLockAcquire(&cfgchange_ch->mutex);
			if (cfgchange_ch->hold_data)
			{
				memcpy(&jobcfg_data, &cfgchange_ch->data, sizeof(JBSCH_ScheduledJobData));

				cfgchange_ch->hold_data = false;
				SpinLockRelease(&cfgchange_ch->mutex);

				if (cfgchange_ch->granularity == JBSCH_CONFIGURATION_CHANGE_GRANULARITY_JOB)
				{
					if (cfgchange_ch->op == JBSCH_CONFIGURATION_CHANGE_OP_INSERT)
					{
						int		free_jobcfg_idx = -1;
						int	i;

						/* search first free */
						for (i = 0; i < n_job_configurations; i++)
						{
							if (jobs_configurations[i].suspended)
							{
								free_jobcfg_idx = i;
								break;
							}
						}

						if (free_jobcfg_idx == -1 && n_job_configurations < MAX_JOBS)
							free_jobcfg_idx = n_job_configurations++;

						if (free_jobcfg_idx != -1)
						{
							memcpy(&jobs_configurations[free_jobcfg_idx], &jobcfg_data, sizeof(JBSCH_ScheduledJobData));
							elog(LOG, "scheduler: store cfg id: %d", jobs_configurations[free_jobcfg_idx].id);
						}
						else
							elog(LOG, "scheduler: cannot to store more job configurations");
					}

					if (cfgchange_ch->op == JBSCH_CONFIGURATION_CHANGE_OP_DELETE)
					{
						int	i;
						int		jobcfg_id;
						bool		found = false;

						jobcfg_id = jobcfg_data.id;

						/* search first free */
						for (i = 0; i < n_job_configurations; i++)
						{
							if (jobs_configurations[i].id == jobcfg_id)
							{
								jobs_configurations[i].suspended = true;
								found = true;
								break;
							}
						}

						if (found)
							elog(LOG, "scheduler: drop configuration: %d", jobcfg_id);
						else
							elog(LOG, "scheduler: cannot to drop job configurations: %d", jobcfg_id);
					}

				}
			}
			else
				/* don't forgot unlock spinlock */
				SpinLockRelease(&cfgchange_ch->mutex);
		}

		if (configuration_is_done)
		{
			int		i;
			TimestampTz	now = GetCurrentTimestamp();

			next_job_start_is_valid = false;

			/* run workers for scheduled statements */
			for (i = 0; i < n_job_configurations; i++)
			{
				JBSCH_ScheduledJob job = &jobs_configurations[i];

				if (!job->suspended)
				{
					if (job->job_start < now)
					{
						sqlexec_prepare_worker(job->id);

						if (job->job_repeat_after == 0)
							job->suspended = true;
						else
						{
							/* ToDo: this can be done after last process of job was processed */

							while (job->job_start < now)
								job->job_start = TimestampTzPlusMilliseconds(job->job_start, job->job_repeat_after * 1000);
						}
					}

					if (!job->suspended)
					{
						if (!next_job_start_is_valid)
						{
							next_job_start = job->job_start;
							next_job_start_is_valid = true;
						}
						else if (next_job_start > job->job_start)
							next_job_start = job->job_start;
					}
				}
			}
		}

		jbsch_DBWorkersPoolCheckAll();

		if (JBSCH_check_timeout || next_job_start_is_valid)
		{
			long	sec;
			int	microsec;
			TimestampTz now = GetCurrentTimestamp();
			TimestampTz nt;

			if (!JBSCH_check_timeout)
				nt = next_job_start;
			else if (!next_job_start_is_valid)
				nt = JBSCH_timeout_fin_time;
			else
				nt = JBSCH_timeout_fin_time < next_job_start ? JBSCH_timeout_fin_time : next_job_start;


			TimestampDifference(now, nt, &sec, &microsec);

			rc = WaitLatch(MyLatch,
					 WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, (1000L * sec) + (microsec / 1000L));
		}
		else
		rc = WaitLatch(MyLatch,
					 WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		if (rc & WL_POSTMASTER_DEATH)
		{
			elog(LOG, "scheduler: postmaster death");
			proc_exit(0);
		}

		CHECK_FOR_INTERRUPTS();

		ResetLatch(MyLatch);
	}

	if (got_sigterm)
	{
		int	i = 0;		/* max number of iterations */

		while (!jbsch_DBworkersPoolSendQuitAll() && i++ < 100)
		{
			rc = WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, 10);

			if (rc & WL_POSTMASTER_DEATH)
			{
				elog(LOG, "scheduler: postmaster death");
				proc_exit(0);
			}

			ResetLatch(MyLatch);
		}
	}

	dsm_detach(dbworker->segment);

	proc_exit(0);
}

void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

/*
 * Entry point for worker loading
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* Register this worker */
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", "scheduler");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = main_worker_handler;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);

	jbsch_register_seclabel();

	RequestAddinShmemSpace(sizeof(JBSCH_ConfigurationChangeChannelData));

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = scheduler_shmem_startup;
}
