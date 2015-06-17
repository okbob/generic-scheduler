/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		jobcfg.c
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

/*
 * Prepare job from tuple
 */
void
jbsch_SetScheduledJob(JBSCH_ScheduledJob job, HeapTupleHeader rec)
{
	Datum		values[Natts_ScheduledJob];
	bool		nulls[Natts_ScheduledJob];
	TupleDesc	rectupdesc;
	HeapTupleData		rectuple;
	Oid		rectuptyp;
	int32		rectuptypmod;

	/* basic magic with tuple unpacking */
	rectuptyp = HeapTupleHeaderGetTypeId(rec);
	rectuptypmod = HeapTupleHeaderGetTypMod(rec);
	rectupdesc = lookup_rowtype_tupdesc(rectuptyp, rectuptypmod);

	Assert(rectupdesc->natts == Natts_ScheduledJob);

	rectuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(rectuple.t_self));
	rectuple.t_tableOid = InvalidOid;
	rectuple.t_data = rec;

	heap_deform_tuple(&rectuple, rectupdesc, values, nulls);
	ReleaseTupleDesc(rectupdesc);

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
	memcpy(job->job_user, NameStr(*DatumGetName(values[Anum_ScheduledJob_job_user - 1])), NAMEDATALEN);
	memcpy(job->job_name, NameStr(*DatumGetName(values[Anum_ScheduledJob_job_name - 1])), NAMEDATALEN);
	memcpy(job->listen_channel, NameStr(*DatumGetName(values[Anum_ScheduledJob_listen_channel - 1])), NAMEDATALEN);

	return;
}

static void
signal_configuration_change_internal(char granularity, char op, HeapTupleHeader rec)
{
	LWLockTranche tranche;
	LWLock lwlock;
	volatile JBSCH_ConfigurationChangeChannel cfgchange_ch = JBSCH_Shm_ConfigurationChangeChannel;

	Assert(cfgchange_ch != NULL);

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
			pg_usleep(1000L);
		}
		else
		{
			cfgchange_ch->granularity = granularity;
			cfgchange_ch->op = op;

			if (granularity == 'j')
				jbsch_SetScheduledJob(&cfgchange_ch->data, rec);

			/* add current database info */
			strcpy(cfgchange_ch->dbname, get_database_name(MyDatabaseId));

			cfgchange_ch->hold_data = true;

			SpinLockRelease(&cfgchange_ch->mutex);
			break;
		}
	}

	/*
	 * anybody can send signal, so pg_signal_backend cannot be used - it is
	 * much more restrictive.
	 */
#ifdef HAVE_SETSID
	if (kill(-cfgchange_ch->scheduler_pid, SIGHUP))
#else
	if (kill(cfgchange_ch->scheduler_pid, SIGHUP))
#endif
	{
		ereport(WARNING,
				(errmsg("could not send SIGHUP to scheduler process %d",
					 cfgchange_ch->scheduler_pid)));
	}

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
