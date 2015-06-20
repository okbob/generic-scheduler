/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		dbwpool.c
 *
 * author: Pavel Stehule, 2015, Czech Republic,
 * licenced under BSD licence
 *
 *-------------------------------------------------------------------------
 */
#include "scheduler.h"

static dlist_head dbworkers_pool = DLIST_STATIC_INIT(dbworkers_pool);
static bool stopped_workers_detected = false;

TimestampTz		JBSCH_timeout_fin_time;
bool			JBSCH_check_timeout = false;

/*
 * Initialize dbworker timeout and check global timeout info
 */
void
jbsch_SetDBWorkerTimeoutAt(JBSCH_DatabaseWorker dbworker, TimestampTz fin_time)
{
	JBSCH_DatabaseWorkerPoolEntry pe = (JBSCH_DatabaseWorkerPoolEntry) dbworker->pool_entry;

	if (pe == NULL)
		elog(ERROR, "Database worker is not in pool.");

	if (JBSCH_check_timeout)
	{
		if (JBSCH_timeout_fin_time > fin_time)
			JBSCH_timeout_fin_time = fin_time;
	}
	else
	{
		JBSCH_timeout_fin_time = fin_time;
		JBSCH_check_timeout = true;
	}

	pe->timeout_fin_time = fin_time;
	pe->check_timeout = true;
}

void
jbsch_SetDBWorkerTimeoutAfter(JBSCH_DatabaseWorker dbworker, int delay_ms)
{
	TimestampTz now;
	TimestampTz fin_time;

	now = GetCurrentTimestamp();
	fin_time = TimestampTzPlusMilliseconds(now, delay_ms);

	jbsch_SetDBWorkerTimeoutAt(dbworker, fin_time);
}

/*
 * repeat timeout with time.
 */
void
jbsch_SetDbWorkerTimeoutRepeat(JBSCH_DatabaseWorker dbworker, int delay_ms)
{
	JBSCH_DatabaseWorkerPoolEntry	pe = (JBSCH_DatabaseWorkerPoolEntry) dbworker->pool_entry;

	jbsch_SetDBWorkerTimeoutAfter(dbworker, delay_ms);
	pe->delay_ms = delay_ms;
}

/*
 * Push living database worker into pool - the pool enforces trigger execution for any
 * data returned from worker. The pool is living on parent side - and ensures all communication
 * with child workers.
 */
void
jbsch_DBWorkersPoolPush(JBSCH_DatabaseWorker dbworker,
				    JBSCH_PoolEntryReceiveTrigger rec_trigger,
				    JBSCH_PoolEntryEventTrigger event_trigger,
				    void *data)
{
	JBSCH_DatabaseWorkerPoolEntry pe = (JBSCH_DatabaseWorkerPoolEntry) palloc0(sizeof(JBSCH_DatabaseWorkerPoolEntryData));

	Assert(dbworker->pool_entry == NULL);		/* it is not in pool already */

	pe->dbworker = dbworker;

	pe->is_living = true;

	pe->rec_trigger = rec_trigger;
	pe->event_trigger = event_trigger;
	pe->data = data;

	/*
	 * pool_entry allows to use fast identification related pool entry
	 * of any watched database workers.
	 */
	dbworker->pool_entry = (struct JBSCH_DatabaseWorkerPoolEntryData *) pe;

	dlist_push_head(&dbworkers_pool, &pe->list_node);
}

void
jbsch_SendQuitDbworker(JBSCH_DatabaseWorker dbworker)
{
	char		*quit_cmd = "\\q";

	JBSCH_DatabaseWorkerMQMode	mq_mode;
	JBSCH_DatabaseWorkerPoolEntry pe;

	mq_mode = dbworker->params->mq_mode;
	pe = (JBSCH_DatabaseWorkerPoolEntry) dbworker->pool_entry;

	if (pe->is_living && pe->is_started && !pe->sent_quit && !pe->is_stopped
		    && ( mq_mode == JBSCH_DBW_MQ_TO_WORKER || mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION ))
	{
		Size length = strlen(quit_cmd);
		int		res;

		res = shm_mq_send(dbworker->out_mq_handle, length, quit_cmd, true);
		if (res == SHM_MQ_DETACHED)
		{
			elog(LOG, "worker '%s' is detached unexpectedly", dbworker->name);
			pe->sent_quit = true; pe->is_detached = true;
		}
		else if (res == SHM_MQ_SUCCESS)
			pe->sent_quit = true;
	}
}

/*
 * returns true, when all living workers in pools accepted "\q" command,
 * else returns false, when call should be repeated for some workers
 */
bool
jbsch_DBworkersPoolSendQuitAll()
{
	dlist_iter	iter;
	char		*quit_cmd = "\\q";
	bool		result = true;

	dlist_foreach(iter, &dbworkers_pool)
	{
		JBSCH_DatabaseWorkerPoolEntry pe;
		JBSCH_DatabaseWorker dbworker;
		JBSCH_DatabaseWorkerMQMode	mq_mode;

		pe = dlist_container(JBSCH_DatabaseWorkerPoolEntryData,
								    list_node,
									    iter.cur);
					
		dbworker = pe->dbworker;
		mq_mode = dbworker->params->mq_mode;

		if (pe->is_living && pe->is_started && !pe->sent_quit && !pe->is_stopped
			    && ( mq_mode == JBSCH_DBW_MQ_TO_WORKER || mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION ))
		{
			Size length = strlen(quit_cmd);
			int		res;

			res = shm_mq_send(dbworker->out_mq_handle, length, quit_cmd, true);
			if (res == SHM_MQ_DETACHED)
			{
				elog(LOG, "worker '%s' is detached unexpectedly", dbworker->name);
				pe->sent_quit = true; pe->is_detached = true;
			}
			else if (res == SHM_MQ_SUCCESS)
				pe->sent_quit = true;
			else
				result = false;
		}
	}

	return result;
}

#define	RUN_DBW_EVENT_TRIGGER(event)				\
if (pe->event_trigger != NULL) (*pe->event_trigger)(pe->dbworker, pe->data, event)

#define RUN_DBW_RECEIVE_TRIGGER()				\
if (pe->rec_trigger != NULL) (*pe->rec_trigger)(pe->dbworker, pe->data, received_bytes, received_data)

/*
 * The basic iteration over pool, change states and run all triggers.
 */
void
jbsch_DBWorkersPoolCheckAll()
{
	dlist_iter	iter;

	TimestampTz	now;

	/*
	 * Save original timeout configuration. When timeout_fin_time will be
	 * same after iteration over
	 */
	if (JBSCH_check_timeout)
		now = GetCurrentTimestamp();
	else
		now = 0;

	dlist_foreach(iter, &dbworkers_pool)
	{
		JBSCH_DatabaseWorkerPoolEntry pe;
		JBSCH_DatabaseWorker dbw;
		JBSCH_DatabaseWorkerMQMode	mq_mode;
		TimestampTz		dbw_timeout_fin_time;
		bool			dbw_check_timeout;

		pe = dlist_container(JBSCH_DatabaseWorkerPoolEntryData,
								    list_node,
									    iter.cur);
					
		dbw = pe->dbworker;
		mq_mode = dbw->params->mq_mode;

		/* store original timeout info, should be changed by triggers */
		if (pe->check_timeout)
		{
			dbw_check_timeout = true;
			dbw_timeout_fin_time = pe->timeout_fin_time;
		}
		else
		{
			dbw_check_timeout = false;
			dbw_timeout_fin_time = 0;
		}

		/* check starting workers */
		if (pe->is_living && !pe->is_started && !pe->is_stopped)
		{
			BgwHandleStatus		status;

			status = jbsch_DatabaseWorkerGetStatus(dbw);
			if (status == BGWH_STARTED)
			{
				pe->is_started = true;
				RUN_DBW_EVENT_TRIGGER(JBSCH_DBW_EVENT_TRIGGER_STARTED);
			}
			else if (status == BGWH_STOPPED)
			{
				pe->is_stopped = true;
				stopped_workers_detected = true;
				RUN_DBW_EVENT_TRIGGER(JBSCH_DBW_EVENT_TRIGGER_STOPPED);

				if (!pe->sent_quit)
					elog(LOG, "worker '%s' failed to start", dbw->name);
			}

			/* do nothing more in this cycle, wait on next latch */
			continue;
		}

		/* check worker's communication */
		if (pe->is_living && pe->is_started && ( !pe->is_detached || !pe->is_stopped )
			    && ( mq_mode == JBSCH_DBW_MQ_FROM_WORKER || mq_mode == JBSCH_DBW_MQ_BOTH_DIRECTION ))
		{
			void	*received_data;
			Size	received_bytes;
			int res;

			res = shm_mq_receive(dbw->in_mq_handle, &received_bytes, &received_data, true);
			if (res == SHM_MQ_SUCCESS)
			{
				RUN_DBW_RECEIVE_TRIGGER();
			}
			else if (res == SHM_MQ_DETACHED)
			{
				pe->is_detached = true;
				RUN_DBW_EVENT_TRIGGER(JBSCH_DBW_EVENT_TRIGGER_DETACHED);

				if (!pe->sent_quit)
					elog(LOG, "worker '%s' unexpectedly detached", dbw->name);

				/* check status early */
				if (jbsch_DatabaseWorkerGetStatus(dbw) == BGWH_STOPPED)
				{
					pe->is_stopped = true;
					stopped_workers_detected = true;
					RUN_DBW_EVENT_TRIGGER(JBSCH_DBW_EVENT_TRIGGER_STOPPED);
				}
			}
		}

		/* check detached workers if living still? */
		if (pe->is_living && pe->is_started && pe->is_detached && !pe->is_stopped)
		{
			if (jbsch_DatabaseWorkerGetStatus(dbw) == BGWH_STOPPED)
			{
				pe->is_stopped = true;
				stopped_workers_detected = true;
				RUN_DBW_EVENT_TRIGGER(JBSCH_DBW_EVENT_TRIGGER_STOPPED);
			}
		}

		/* run timeout trigger */
		if (pe->is_living && pe->is_started && !pe->is_stopped
			    && dbw_check_timeout && dbw_timeout_fin_time <= now)
		{
			RUN_DBW_EVENT_TRIGGER(JBSCH_DBW_EVENT_TRIGGER_TIME);

			/* is still there previous timeout? */
			if (pe->delay_ms > 0)
			{
				/*
				 * Append delay only if current timeout was processed
				 */
				if (dbw_timeout_fin_time == pe->timeout_fin_time)
					dbw_timeout_fin_time = TimestampTzPlusMilliseconds(pe->timeout_fin_time, pe->delay_ms);
				else
					dbw_timeout_fin_time = pe->timeout_fin_time;

				while (dbw_timeout_fin_time <= now)
					dbw_timeout_fin_time = TimestampTzPlusMilliseconds(dbw_timeout_fin_time, pe->delay_ms);

				jbsch_SetDBWorkerTimeoutAt(dbw, dbw_timeout_fin_time);
			}
			else if (dbw_timeout_fin_time == pe->timeout_fin_time)
				pe->check_timeout = false;
		}
	}

	/* recheck global timeout */
	if (JBSCH_check_timeout)
	{
		TimestampTz		next_timeout_fin_time;
		bool			dbw_check_timeout = false;

		/* find next timeout event */
		dlist_foreach(iter, &dbworkers_pool)
		{
			JBSCH_DatabaseWorkerPoolEntry pe;

			pe = dlist_container(JBSCH_DatabaseWorkerPoolEntryData,
									    list_node,
										    iter.cur);

			if (pe->is_living && pe->is_started && !pe->is_stopped
				    && pe->check_timeout)
			{
				if (!dbw_check_timeout)
				{
					next_timeout_fin_time = pe->timeout_fin_time;
					dbw_check_timeout = true;
				}
				else if (pe->timeout_fin_time < next_timeout_fin_time)
					next_timeout_fin_time = pe->timeout_fin_time;
			}
		}

		if (!dbw_check_timeout)
			JBSCH_check_timeout = false;
		else
			JBSCH_timeout_fin_time = next_timeout_fin_time;
	}
}
