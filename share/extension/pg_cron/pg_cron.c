/* -------------------------------------------------------------------------
 *
 * pg_cron.c
 *
 * Portions Copyright (C) 2013, PostgreSQL Global Development Group
 * Portions Copyright (C) 2013, Eugene Karpenko <jhon.karpenko@gmail.com>
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	pg_part_naptime = 10;
static char * 	pg_part_database = NULL;
static char * 	pg_part_procedure = NULL;
static char * 	pg_part_schema = NULL;

typedef struct worktable
{
	const char *schema;
	const char *name;
} worktable;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_part_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_part_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

static void
pg_part_main(Datum main_arg)
{
	StringInfoData buf;
	initStringInfo(&buf);

	elog(DEBUG1, "pg_part: start");

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_part_sighup);
	pqsignal(SIGTERM, pg_part_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(pg_part_database, NULL);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			ret;
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   pg_part_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Start a transaction on which we can run queries.  Note that each
		 * StartTransactionCommand() call should be preceded by a
		 * SetCurrentStatementStartTimestamp() call, which sets both the time
		 * for the statement we're about the run, and also the transaction
		 * start time.	Also, each other query sent to SPI should probably be
		 * preceded by SetCurrentStatementStartTimestamp(), so that statement
		 * start time is always up to date.
		 *
		 * The SPI_connect() call lets us run queries through the SPI manager,
		 * and the PushActiveSnapshot() call creates an "active" snapshot
		 * which is necessary for queries to have MVCC data to work on.
		 *
		 * The pgstat_report_activity() call makes our activity visible
		 * through the pgstat views.
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, buf.data);

		/* We can now execute queries via SPI */
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT 1 "
						 "FROM pg_proc p "
						 "JOIN pg_namespace n ON p.pronamespace = n.oid "
						 "WHERE n.nspname = '%s' AND p.proname = '%s' "
						 "LIMIT 1",
						 pg_part_schema,
						 pg_part_procedure);

		elog(DEBUG1, "pg_part: looking for the procedure");
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "cannot query the database: error code %d", ret);

		if (SPI_processed > 0)
			{
				resetStringInfo(&buf);
				appendStringInfo(&buf, "SELECT %s.%s()",
								 pg_part_schema,
								 pg_part_procedure);
				elog(DEBUG1, "pg_part: firing the procedure");
				ret = SPI_execute(buf.data, false, 0);

				if (ret != SPI_OK_SELECT)
					elog(FATAL, "cannot query the database: error code %d", ret);
			}
		else
			{
				elog(DEBUG1, "pg_part: procedure %s.%s not found in database %s",
					 pg_part_schema,
					 pg_part_procedure,
					 pg_part_database);
			}

		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	proc_exit(0);
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* get the configuration */
	DefineCustomIntVariable("pg_part.naptime",
							"Duration between each spark (in seconds).",
							NULL,
							&pg_part_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomStringVariable("pg_part.database",
							   "Name of the database where the spark procedure is.",
							   NULL,
							   &pg_part_database,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);
	DefineCustomStringVariable("pg_part.schema",
							   "Name of the schema where the spark procedure is.",
							   NULL,
							   &pg_part_schema,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);
	DefineCustomStringVariable("pg_part.procedure",
							   "Name of the spark procedure.",
							   NULL,
							   &pg_part_procedure,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);

	/* set up worker data */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	worker.bgw_main = pg_part_main;
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_part");

	/* register worker */
	RegisterBackgroundWorker(&worker);
}
