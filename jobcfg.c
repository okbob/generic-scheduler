/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		jobcfg.c
 *
 * author: Pavel Stehule, 2015, Czech Republic,
 * licenced under BSD licence
 *
 *-------------------------------------------------------------------------
 */
#include "scheduler.h"

/*
 * shared variable used for communication - scheduler <= any client process.
 */
JBSCH_ConfigurationChangeChannel JBSCH_Shm_ConfigurationChangeChannel = NULL;

TimeOffset
jbsch_to_timeoffset(Interval *interval)
{
	int			year,
				month,
				day;
	TimeOffset	span;

	year = interval->month / MONTHS_PER_YEAR;
	month = interval->month % MONTHS_PER_YEAR;
	day = interval->day;

#ifdef HAVE_INT64_TIMESTAMP
	span = ((INT64CONST(365250000) * year + INT64CONST(30000000) * month +
			 INT64CONST(1000000) * day) * INT64CONST(86400)) +
		interval->time;
	span /= USECS_PER_SEC;
#else
	span = (DAYS_PER_YEAR * year + (double) DAYS_PER_MONTH * month + day) * SECS_PER_DAY + interval->time;
#endif

	return span;
}

void
jbsch_SetScheduledJob(JBSCH_ScheduledJob job, HeapTuple tuple, TupleDesc tupdesc)
{
	Datum		values[Natts_ScheduledJob];
	bool		nulls[Natts_ScheduledJob];
	TimestampTz now;

	now = GetCurrentTimestamp();


	Assert(tupdesc->natts == Natts_ScheduledJob);

	heap_deform_tuple(tuple, tupdesc, values, nulls);

	/* copy values to output */
	job->id = DatumGetInt32(values[Anum_ScheduledJob_id -1]);
	job->suspended = DatumGetBool(values[Anum_ScheduledJob_suspended - 1]);
	job->max_workers = DatumGetInt32(values[Anum_ScheduledJob_max_workers - 1]);
	job->run_workers = DatumGetInt32(values[Anum_ScheduledJob_run_workers - 1]);
	job->idle_timeout = jbsch_to_timeoffset(DatumGetIntervalP(values[Anum_ScheduledJob_idle_timeout - 1]));
	job->life_time = jbsch_to_timeoffset(DatumGetIntervalP(values[Anum_ScheduledJob_life_time - 1]));
	job->job_start = DatumGetTimestampTz(values[Anum_ScheduledJob_job_start - 1]);
	job->job_repeat_after = jbsch_to_timeoffset(DatumGetIntervalP(values[Anum_ScheduledJob_job_repeat_after - 1]));
	job->job_start_timeout = jbsch_to_timeoffset(DatumGetIntervalP(values[Anum_ScheduledJob_job_start_timeout - 1]));
	job->job_timeout = jbsch_to_timeoffset(DatumGetIntervalP(values[Anum_ScheduledJob_job_timeout - 1]));
	job->job_start_delay = jbsch_to_timeoffset(DatumGetIntervalP(values[Anum_ScheduledJob_job_start_delay - 1]));
	memcpy(&job->job_user, DatumGetName(values[Anum_ScheduledJob_job_user - 1]), NAMEDATALEN);
	memcpy(&job->job_name, DatumGetName(values[Anum_ScheduledJob_job_name - 1]), NAMEDATALEN);
	memcpy(&job->listen_channel, DatumGetName(values[Anum_ScheduledJob_listen_channel - 1]), NAMEDATALEN);

	/* recheck time for jobs and protect agains unwanted runs */
	if (!job->suspended && *NameStr(job->listen_channel) == '\0')
	{
		if (job->job_start < now)
		{
			/* ToDo: Can be runned tasks in job_start_timeout */
			if (job->job_repeat_after == 0)
				job->suspended = true;
			else
			{
				while (job->job_start < now)
					job->job_start = TimestampTzPlusMilliseconds(job->job_start, job->job_repeat_after * 1000);
			}
		}
	}

	return;
}

/*
 * Prepare job from tuple
 */
void
jbsch_SetScheduledJobRecord(JBSCH_ScheduledJob job, HeapTupleHeader rec)
{
	TupleDesc	rectupdesc;
	HeapTupleData		rectuple;
	Oid		rectuptyp;
	int32		rectuptypmod;

	/* basic magic with tuple unpacking */
	rectuptyp = HeapTupleHeaderGetTypeId(rec);
	rectuptypmod = HeapTupleHeaderGetTypMod(rec);
	rectupdesc = lookup_rowtype_tupdesc(rectuptyp, rectuptypmod);

	rectuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(rectuple.t_self));
	rectuple.t_tableOid = InvalidOid;
	rectuple.t_data = rec;

	jbsch_SetScheduledJob(job, &rectuple, rectupdesc);

	ReleaseTupleDesc(rectupdesc);

	return;
}

/*
 * send sighup to target pid, anybody can do it.
 */
static void
send_sighup(int pid)
{
#ifdef HAVE_SETSID
	if (kill(-pid, SIGHUP))
#else
	if (kill(pid, SIGHUP))
#endif
	{
		ereport(ERROR,
				(errmsg("could not send SIGHUP to scheduler process %d", pid)));
	}
}

static void
signal_configuration_change_internal(char granularity, char op, HeapTupleHeader rec)
{
	LWLockTranche tranche;
	LWLock lwlock;
	volatile JBSCH_ConfigurationChangeChannel cfgchange_ch = JBSCH_Shm_ConfigurationChangeChannel;
	int		i = 0;		/* protection against deadlock */
	bool			aux_sighup = false;

	/* when master process is not active */
	if (cfgchange_ch == NULL)
	{
		elog(WARNING, "the scheduler process is not active");
		return;
	}

	LWLockRegisterTranche(cfgchange_ch->lock_tranche_id, &tranche);
	LWLockInitialize(&lwlock, cfgchange_ch->lock_tranche_id);

	LWLockAcquire(&lwlock, LW_EXCLUSIVE);

	for (;;)
	{
		SpinLockAcquire(&cfgchange_ch->mutex);
		if (cfgchange_ch->hold_data)
		{
			SpinLockRelease(&cfgchange_ch->mutex);
			CHECK_FOR_INTERRUPTS();
			pg_usleep(50 * 1000L);

			/* After 2 sec, resend SIGHUP, it is safe for reading */
			if (++i > 40 && !aux_sighup)
			{
				aux_sighup = true;
				send_sighup(cfgchange_ch->scheduler_pid);
			}
			
			/* After 5 sec fail */
			if (i > 100)
				ereport(ERROR,
					(errmsg("could not send data to scheduler process %d",
									 cfgchange_ch->scheduler_pid),
					 errdetail("scheduler is not active reader")));
		}
		else
		{
			cfgchange_ch->granularity = granularity;
			cfgchange_ch->op = op;

			if (granularity == 'j')
				jbsch_SetScheduledJobRecord(&cfgchange_ch->data, rec);

			/* add current database info */
			strcpy(cfgchange_ch->dbname, get_database_name(MyDatabaseId));

			cfgchange_ch->hold_data = true;

			SpinLockRelease(&cfgchange_ch->mutex);
			break;
		}
	}

	send_sighup(cfgchange_ch->scheduler_pid);

	LWLockRelease(&lwlock);
}

/*
 * emits local changes to central scheduler process. The communication is done via schared
 * variable. A writing (possible longer waiting) is protected by LWLock, reading is protected
 * by mutex (for ensuring consistent reading).
 */ 
PG_FUNCTION_INFO_V1(jbsch_signal_configuration_change);

Datum
jbsch_signal_configuration_change(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	char		op = PG_GETARG_CHAR(1);

	signal_configuration_change_internal(
			 JBSCH_CONFIGURATION_CHANGE_GRANULARITY_JOB,
			 op, rec);

	PG_RETURN_VOID();
}

/*
 * simple security label - it is only one way, how to simply work with global
 * data - via shared sec labels. It can allow simply suspend or activate all
 * jobs in database. Only super user or database owner can do it.
 */
static void
scheduler_object_relabel(const ObjectAddress *object, const char *seclabel)
{
	if (!superuser() || !pg_database_ownercheck(MyDatabaseId, GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("only superuser or database owner can set '%s' label", seclabel)));

	if (seclabel == NULL || strcmp(seclabel, "nonactive") == 0)
	{
		signal_configuration_change_internal(
				 JBSCH_CONFIGURATION_CHANGE_GRANULARITY_DATABASE,
				 JBSCH_CONFIGURATION_CHANGE_OP_NONACTIVATE, NULL);
		return;
	}
	else if (strcmp(seclabel, "active") == 0)
	{
		signal_configuration_change_internal(
				 JBSCH_CONFIGURATION_CHANGE_GRANULARITY_DATABASE,
				 JBSCH_CONFIGURATION_CHANGE_OP_ACTIVATE, NULL);
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_NAME),
			 errmsg("'%s' is not a valid security label", seclabel)));
}

void
jbsch_register_seclabel(void)
{
	register_label_provider("jobscheduler", scheduler_object_relabel);
}
