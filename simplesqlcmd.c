/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		simplesqlcmd.c
 *
 * author: Pavel Stehule, 2015, Czech Republic,
 * licenced under BSD licence
 *
 *-------------------------------------------------------------------------
 */
#include "scheduler.h"

/*
 * Copy text of SQL statement to shared memory related buffer.
 * The size of SQL statement is limited to 2KB now.
 */
void
jbsch_SetShortSQLCmd(JBSCH_DatabaseWorker dbw, const char *sqlstr)
{
	bool	consumed;

	if (!dbw->params->use_sqlcmd)
	{
		elog(LOG, "Cannot to set short SQL cmd (worker has not support short sql)");
		proc_exit(1);
	}

	SpinLockAcquire(&dbw->sqlcmd->mutex);
	consumed = dbw->sqlcmd->consumed;
	SpinLockRelease(&dbw->sqlcmd->mutex);

	if (!consumed)
		elog(LOG, "previous short SQL cmd was not consumed");

	SpinLockAcquire(&dbw->sqlcmd->mutex);
	StrNCpy((char *) &dbw->sqlcmd->sqlstr, sqlstr, JBSCH_MAX_SQL_STRLEN);
	dbw->sqlcmd->consumed = false;
	SpinLockRelease(&dbw->sqlcmd->mutex);
}

char *
jbsch_FetchShortSQLCmd(JBSCH_DatabaseWorker dbw)
{
	bool	consumed;
	char	*result;

	if (!dbw->params->use_sqlcmd)
	{
		elog(LOG, "Cannot to set short SQL cmd (worker has not support short sql)");
		proc_exit(1);
	}

	SpinLockAcquire(&dbw->sqlcmd->mutex);
	consumed = dbw->sqlcmd->consumed;
	SpinLockRelease(&dbw->sqlcmd->mutex);

	if (consumed)
		elog(LOG, "short SQL cmd was consumed already");

	SpinLockAcquire(&dbw->sqlcmd->mutex);
	result = pstrdup(dbw->sqlcmd->sqlstr);
	dbw->sqlcmd->consumed = true;
	SpinLockRelease(&dbw->sqlcmd->mutex);

	return result;
}

/*
 * In the end the SPI should not be used due limits - it cannot to run
 * DO statement. "ERROR:  DO is not allowed in a non-volatile function"
 */
void
jbsch_ExecuteSQL(char *sqlstr)
{
	ResourceOwner		oldResourceOwner;

	oldResourceOwner = CurrentResourceOwner; 

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, sqlstr);

	PG_TRY();
	{
		SPI_execute(sqlstr, false, 1);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
	}
	PG_CATCH();
	{
		EmitErrorReport();
		AbortCurrentTransaction();
		pgstat_report_activity(STATE_IDLE, NULL);
		FlushErrorState();
	}
	PG_END_TRY();

	CurrentResourceOwner = oldResourceOwner; 
}
