/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		scheduler.c
 *
 *-------------------------------------------------------------------------
 */
#include "scheduler.h"

/*
 * Todo:
 *      --- 1. fix SQL execution
 *      -- 2. security label and configuration load
 *      3. Throtling and worker state machine
 *      4. Notify event listeners
 *      6. split to files and cleaning
 *      7. tests
 *      8. publishing
 */


/* related to shared memory CCC */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static int loops = 0;

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

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
		if (res == SHM_MQ_DETACHED)
		{
			elog(LOG, "worker: parent died");
			break;
		}

		/*
		 * Should not be done
		 */
		if (res != SHM_MQ_SUCCESS)
		{
			elog(LOG, "worker: cannot to get data");
			break;
		}

		if (strcmp(received_data, "\\q") == 0)
		{
			elog(LOG, "worker: received QUIT command");
			break;
		}

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

			jbsch_ExecuteSQL(sqlstr);
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
	int rc;

	int	t1 = 1000;
	int	t2 = 3000;

	/* Connect to database, necessary to get PGPROC */
	/* attention - it reset CurrentResourceOwner to NULL */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "database scheduler");

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

	jbsch_DBWorkersPoolPush(dbworker, simple_dbw_receive_trigger, simple_dbw_event_trigger, &t1);
	jbsch_DBWorkersPoolPush(dbworker2, simple_dbw_receive_trigger, simple_dbw_event_trigger, &t2);

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
			JBSCH_ScheduledJobData	sjd;

			got_sighup = false;

			SpinLockAcquire(&cfgchange_ch->mutex);
			if (cfgchange_ch->hold_data)
			{
				memcpy(&sjd, &cfgchange_ch->data, sizeof(JBSCH_ScheduledJobData));
				cfgchange_ch->hold_data = false;

				elog(LOG, "scheduler: job_user: %s", sjd.job_user);
			}
			SpinLockRelease(&cfgchange_ch->mutex);
		}

		jbsch_DBWorkersPoolCheckAll();

		if (JBSCH_check_timeout)
		{
			long	sec;
			int	microsec;
			TimestampTz now = GetCurrentTimestamp();

			TimestampDifference(now, JBSCH_timeout_fin_time, &sec, &microsec);

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
