/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		dbworker.c
 *
 *-------------------------------------------------------------------------
 */
#include "scheduler.h"

/*
 * Returns state of DatabaseWorker process
 */
BgwHandleStatus
jbsch_DatabaseWorkerGetStatus(JBSCH_DatabaseWorker dbworker)
{
	pid_t		pid;

	Assert(dbworker->handle != NULL);

	return GetBackgroundWorkerPid(dbworker->handle, &pid);
}

/*
 * Create new database worker process and prepare communication to parent
 *
 */
JBSCH_DatabaseWorker
jbsch_NewDatabaseWorker(char *worker_name, char *dbname, char *user,
		         Size data_size, int keys, Size shm_mq_size,
		         JBSCH_DatabaseWorkerMQMode mq_mode,
		         int restart_time,
		         bgworker_main_type bgw_main,
		         int other_options)
{
	dsm_segment	*segment;
	shm_toc_estimator	e;
	shm_toc			*toc;
	Size				segsize;
	BackgroundWorker	 worker;
	JBSCH_DatabaseWorker		 dbworker;
	volatile JBSCH_DatabaseWorkerParams	 params;
	shm_mq		*in_mq = NULL;
	shm_mq		*out_mq = NULL;

	dbworker = (JBSCH_DatabaseWorker) palloc((sizeof(JBSCH_DatabaseWorkerData)));

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(JBSCH_DatabaseWorkerParamsData));
	shm_toc_estimate_chunk(&e, data_size);

	if (other_options & JBSCH_DBWORKER_SHORT_SQLCMD)
		shm_toc_estimate_chunk(&e, sizeof(JBSCH_SQLCmdData));
	

	if (mq_mode != JBSCH_DBW_MQ_NONE)
	{
		Size		mq_size = 0;

		mq_size = shm_mq_size > shm_mq_minimum_size ? shm_mq_size : shm_mq_minimum_size;

		shm_toc_estimate_chunk(&e, mq_size);
		if (mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION)
			shm_toc_estimate_chunk(&e, mq_size);
	}

	shm_toc_estimate_keys(&e, keys + 4);			/* four keys are use for system */

	segsize = shm_toc_estimate(&e);

	segment = dsm_create(shm_toc_estimate(&e), DSM_CREATE_NULL_IF_MAXSEGMENTS);
	if (segment == NULL)
		elog(ERROR, "unable map dynamic memory segment");

	toc = shm_toc_create(JBSCH_SCHEDULER_SHM_MQ_MAGIC, dsm_segment_address(segment), segsize);
	if (toc == NULL)
		elog(ERROR, "bad magic number in dynamic memory segment");

	params = (JBSCH_DatabaseWorkerParams) shm_toc_allocate(toc, (sizeof(JBSCH_DatabaseWorkerParamsData)));

	if (dbname != NULL)
	{
		StrNCpy(params->dbname, dbname, NAMEDATALEN);
		params->use_default_database = false;
	}
	else
		params->use_default_database = true;

	if (user != NULL)
	{
		StrNCpy(params->user, user, NAMEDATALEN);
		params->use_default_user = false;
	}
	else
		params->use_default_user = true;

	shm_toc_insert(toc, 0, (void *) params);

	if (mq_mode == JBSCH_DBW_MQ_FROM_WORKER || mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION)
	{
		in_mq = shm_mq_create(shm_toc_allocate(toc, data_size / 3), data_size / 3);
		shm_mq_set_receiver(in_mq, MyProc);
		shm_toc_insert(toc, 1, (void *) in_mq);
	}

	if (mq_mode == JBSCH_DBW_MQ_TO_WORKER || mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION)
	{
		out_mq = shm_mq_create(shm_toc_allocate(toc, data_size / 3), data_size / 3);
		shm_mq_set_sender(out_mq, MyProc);
		shm_toc_insert(toc, 2, (void *) out_mq);
	}

	params->mq_mode = mq_mode;
	params->use_sqlcmd = false;

	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = bgw_main;
	worker.bgw_restart_time = restart_time;
	worker.bgw_main_arg = (Datum) UInt32GetDatum(dsm_segment_handle(segment));
	worker.bgw_notify_pid = MyProcPid;

	dbworker->name = pstrdup(worker_name);
	dbworker->segment = segment;
	dbworker->toc = toc;
	dbworker->params = params;
	dbworker->parent = NULL;		/* this is not child process */

	RegisterDynamicBackgroundWorker(&worker, &dbworker->handle);

	if (in_mq != NULL)
		dbworker->in_mq_handle = shm_mq_attach(in_mq, segment, dbworker->handle);

	if (out_mq != NULL)
		dbworker->out_mq_handle = shm_mq_attach(out_mq, segment, dbworker->handle);

	if (other_options & JBSCH_DBWORKER_SHORT_SQLCMD)
	{
		JBSCH_SQLCmd		sqlcmd;

		sqlcmd = (JBSCH_SQLCmd) shm_toc_allocate(toc, (sizeof(JBSCH_SQLCmdData)));

		SpinLockInit(&sqlcmd->mutex);
		sqlcmd->consumed = false;

		shm_toc_insert(toc, 3, (void *) sqlcmd);

		dbworker->sqlcmd = sqlcmd;
		dbworker->params->use_sqlcmd = true;
	}

	dbworker->pool_entry = NULL;

	return dbworker;
}

/*
 * Initialize child process - ResourceOwner should be already
 */
JBSCH_DatabaseWorker
jbsch_SetupDatabaseWorker(dsm_segment *segment)
{
	JBSCH_DatabaseWorker		 dbworker;
	volatile JBSCH_DatabaseWorkerParams	 params;
	shm_mq		 *in_mq;
	shm_mq		 *out_mq;

	dbworker = (JBSCH_DatabaseWorker) palloc((sizeof(JBSCH_DatabaseWorkerData)));

	dbworker->segment = segment;
	dbworker->toc = shm_toc_attach(JBSCH_SCHEDULER_SHM_MQ_MAGIC, dsm_segment_address(segment));
	if (dbworker->toc == NULL)
		elog(ERROR, "bad magic number in dynamic memory segment");

	params = (JBSCH_DatabaseWorkerParams) shm_toc_lookup(dbworker->toc, 0);

	/* Connect to database */
	BackgroundWorkerInitializeConnection(params->use_default_database ? "postgres" : params->dbname,
					     params->use_default_user ? NULL : params->user);

	dbworker->parent = BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid);
	dbworker->params = params;
	dbworker->handle = NULL;

	if (params->mq_mode == JBSCH_DBW_MQ_FROM_WORKER || params->mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION)
	{
		out_mq = (shm_mq *) shm_toc_lookup(dbworker->toc, 1);
		shm_mq_set_sender(out_mq, MyProc);
		dbworker->out_mq_handle = shm_mq_attach(out_mq, segment, NULL);
	}

	if (params->mq_mode == JBSCH_DBW_MQ_TO_WORKER || params->mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION)
	{
		in_mq = (shm_mq *) shm_toc_lookup(dbworker->toc, 2);
		shm_mq_set_receiver(in_mq, MyProc);
		dbworker->in_mq_handle = shm_mq_attach(in_mq, segment, NULL);
	}

	if (params->use_sqlcmd)
		dbworker->sqlcmd = (JBSCH_SQLCmd) shm_toc_lookup(dbworker->toc, 3);

	/* echo to parent - the child is ready */
	SetLatch(&dbworker->parent->procLatch);

	return dbworker;
}
