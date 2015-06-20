/*------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *		scheduler/scheduler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "commands/seclabel.h"
#include "executor/spi.h"
#include "lib/ilist.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

/**************************************************************
 *
 *          jobcfg.c
 *
 **************************************************************
 */
#define Natts_ScheduledJob				16
#define Anum_ScheduledJob_id			1
#define Anum_ScheduledJob_suspended		2
#define Anum_ScheduledJob_max_workers		3
#define Anum_ScheduledJob_run_workers		4
#define Anum_ScheduledJob_idle_timeout		5
#define Anum_ScheduledJob_life_time		6
#define Anum_ScheduledJob_job_start		7
#define Anum_ScheduledJob_job_repeat_after	8
#define Anum_ScheduledJob_job_start_timeout	9
#define Anum_ScheduledJob_job_timeout		10
#define Anum_ScheduledJob_job_user		11
#define Anum_ScheduledJob_job_name		12
#define Anum_ScheduledJob_job_cmd		13
#define Anum_ScheduledJob_job_trigger_fun	14
#define Anum_ScheduledJob_listen_channel	15
#define Anum_ScheduledJob_job_start_delay	16

/*
 * basic job configuration - doesn't contains SQL statements due
 * possible bigger size to hold it in memory.
 */
typedef struct
{
	int		id;
	bool		suspended;
	int		max_workers;
	int		run_workers;
	TimeOffset	idle_timeout;
	TimeOffset	life_time;
	TimestampTz	job_start;
	TimeOffset	job_repeat_after;
	TimeOffset	job_start_timeout;
	TimeOffset	job_timeout;
	char		job_user[NAMEDATALEN];
	char		job_name[NAMEDATALEN];
	char		listen_channel[NAMEDATALEN];
	TimeOffset	job_start_delay;
} JBSCH_ScheduledJobData;

typedef JBSCH_ScheduledJobData	*JBSCH_ScheduledJob;

#define JBSCH_CONFIGURATION_CHANGE_GRANULARITY_JOB		'j'
#define JBSCH_CONFIGURATION_CHANGE_GRANULARITY_DATABASE		'd'

#define JBSCH_CONFIGURATION_CHANGE_OP_INSERT			'i'
#define JBSCH_CONFIGURATION_CHANGE_OP_DELETE			'd'
#define JBSCH_CONFIGURATION_CHANGE_OP_ACTIVATE			'a'
#define JBSCH_CONFIGURATION_CHANGE_OP_NONACTIVATE		'n'

/*
 * Shared memory used for pushing configuration from any custom process to
 * central scheduler. It is executed from job_schedule triggers.
 */
typedef struct
{
	int			scheduler_pid;		/* consumer of SIGHUP signal */
	int			lock_tranche_id;	/* light weight lock tranche id for write synchronization*/
	slock_t			mutex;			/* short lock for ensuring of consistent read */
	bool			hold_data;		/* true if var data holds unreaden data */
	JBSCH_ScheduledJobData	data;
	char			dbname[NAMEDATALEN];
	char		op;			/* i insert, u update, a activate, n nonactivate  */
	char		granularity;		/* j job, d database */
} JBSCH_ConfigurationChangeChannelData;

typedef JBSCH_ConfigurationChangeChannelData *JBSCH_ConfigurationChangeChannel;

extern JBSCH_ConfigurationChangeChannel JBSCH_Shm_ConfigurationChangeChannel;

/**************************************************************
 *
 *          simplesqlcmd.c
 *
 **************************************************************
 */
#define			JBSCH_MAX_SQL_STRLEN		2048

/*
 * This structure is used for execution some shorted SQL statement
 * from parent process on child process. It is used mainly for
 * testing purpouses.
 */
typedef struct
{
	slock_t		mutex;
	char		sqlstr[JBSCH_MAX_SQL_STRLEN];
	bool		consumed;
} JBSCH_SQLCmdData;

typedef JBSCH_SQLCmdData	*JBSCH_SQLCmd;


/**************************************************************
 *
 *          dbworker.c
 *
 **************************************************************
 */
#define		JBSCH_SCHEDULER_SHM_MQ_MAGIC		0x79fb3448

typedef enum JBSCH_DatabaseWorkerMQMode
{
	JBSCH_DBW_MQ_NONE,
	JBSCH_DBW_MQ_TO_WORKER,
	JBSCH_DBW_MQ_FROM_WORKER,
	JBSCH_DBW_MQ_BOTH_DIRECTION
} JBSCH_DatabaseWorkerMQMode;

/*
 * Shared data between parent and child process
 */
typedef struct
{
	char	 dbname[NAMEDATALEN];
	char	 user[NAMEDATALEN];
	bool	 use_default_database;
	bool	 use_default_user;
	bool	 use_sqlcmd;

	JBSCH_DatabaseWorkerMQMode mq_mode;
} JBSCH_DatabaseWorkerParamsData;

typedef JBSCH_DatabaseWorkerParamsData *JBSCH_DatabaseWorkerParams;

/*
 * The struct DatabaseWorker holds all info necessary for implementation
 * bidirectional communication between parent and child process.
 *
 * Note: Todo: It is used for parent and child side, what is probably
 * wrong - should be divided on DatabaseWorker and DatabaseWorkerController.
 */
typedef struct
{
	char		 *name;
	dsm_segment	 *segment;
	shm_toc		 *toc;
	JBSCH_DatabaseWorkerParams		 params;
	BackgroundWorkerHandle 			 *handle;
	JBSCH_SQLCmd				sqlcmd;
	PGPROC			*parent;
	shm_mq_handle		*in_mq_handle;
	shm_mq_handle		*out_mq_handle;
	struct JBSCH_DatabaseWorkerPoolEntryData *pool_entry;		/* Non NULL when dbworker is handled by pool */
} JBSCH_DatabaseWorkerData;

typedef JBSCH_DatabaseWorkerData *JBSCH_DatabaseWorker;

#define JBSCH_DBWORKER_SHORT_SQLCMD			0x0001

/**************************************************************
 *
 *          dbwpool.c
 *
 **************************************************************
 */
typedef enum JBSCH_DatabaseWorkerEvent
{
	JBSCH_DBW_EVENT_TRIGGER_STARTED,
	JBSCH_DBW_EVENT_TRIGGER_STOPPED,
	JBSCH_DBW_EVENT_TRIGGER_DETACHED,
	JBSCH_DBW_EVENT_TRIGGER_TIME
} JBSCH_DatabaseWorkerEvent;

typedef void (*JBSCH_PoolEntryReceiveTrigger) (JBSCH_DatabaseWorker dbworker, void *data, Size bytes, void *received_data);
typedef void (*JBSCH_PoolEntryEventTrigger) (JBSCH_DatabaseWorker dbworker, void *data, JBSCH_DatabaseWorkerEvent event);

/*
 * pool is a store for some general state data and related callbacks.
 * When some event in pool is detected, then related callback is executed.
 */
typedef struct
{
	JBSCH_DatabaseWorker		dbworker;		/* it works like PK too */
	bool			is_living;		/* true when worker was not finished */
	bool			is_started;		/* true when worker was started successfuly */
	bool			is_detached;		/* true when worker was detached */
	bool			is_stopped;		/* true when worker was stopped */
	bool			sent_quit;		/* true when worker was requested to quit */
	void			*data;			/* private data */
	TimestampTz		timeout_fin_time;	/* nonzero when time event is requested */
	int			delay_ms;		/* when nonzero a interval for repeat interval */
	bool			check_timeout;		/* true, when timeout active */
	JBSCH_PoolEntryReceiveTrigger		rec_trigger;	/* receive data trigger */
	JBSCH_PoolEntryEventTrigger		event_trigger;	/* event trigger - start, detach, time .. */
	dlist_node			list_node;
} JBSCH_DatabaseWorkerPoolEntryData;


typedef JBSCH_DatabaseWorkerPoolEntryData *JBSCH_DatabaseWorkerPoolEntry;

extern TimestampTz	JBSCH_timeout_fin_time;
extern bool		JBSCH_check_timeout;

/*
 * jobcfg.c
 *
 */
TimeOffset jbsch_to_timeoffset(Interval *interval);
void jbsch_SetScheduledJob(JBSCH_ScheduledJob job, HeapTuple tuple, TupleDesc tupdesc);
void jbsch_SetScheduledJobRecord(JBSCH_ScheduledJob job, HeapTupleHeader rec);
void jbsch_register_seclabel(void);

/*
 * simplesqlcmd.c
 *
 */
void jbsch_SetShortSQLCmd(JBSCH_DatabaseWorker dbw, const char *sqlstr);
char *jbsch_FetchShortSQLCmd(JBSCH_DatabaseWorker dbw);
void jbsch_ExecuteSQL(char *sqlstr);

/*
 * dbworker.c
 *
 */
BgwHandleStatus jbsch_DatabaseWorkerGetStatus(JBSCH_DatabaseWorker dbworker);

JBSCH_DatabaseWorker jbsch_NewDatabaseWorker(char *worker_name, char *dbname, char *user,
					         Size data_size, int keys, Size shm_mq_size,
						         JBSCH_DatabaseWorkerMQMode mq_mode,
							         int restart_time, bgworker_main_type bgw_main,
								         int other_options);

JBSCH_DatabaseWorker jbsch_SetupDatabaseWorker(dsm_segment *segment);

/*
 * dbwpool.c
 *
 */
void jbsch_SetDBWorkerTimeoutAt(JBSCH_DatabaseWorker dbworker, TimestampTz fin_time);
void jbsch_SetDBWorkerTimeoutAfter(JBSCH_DatabaseWorker dbworker, int delay_ms);
void jbsch_SetDbWorkerTimeoutRepeat(JBSCH_DatabaseWorker dbworker, int delay_ms);

void jbsch_DBWorkersPoolPush(JBSCH_DatabaseWorker dbworker,
				    JBSCH_PoolEntryReceiveTrigger rec_trigger, JBSCH_PoolEntryEventTrigger event_trigger,
						    void *data);

void jbsch_SendQuitDbworker(JBSCH_DatabaseWorker dbworker);
bool jbsch_DBworkersPoolSendQuitAll(void);
void jbsch_DBWorkersPoolCheckAll(void);
