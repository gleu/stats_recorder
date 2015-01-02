/* -------------------------------------------------------------------------
 *
 * stats_recorder_spi.c
 *		Background worker code that gets a periodic copy of stats.
 *
 *
 * Original code from PostgreSQL Global Development Group
 *   (worker_spi contrib module)
 *
 * Copyright (c) 2012-2015, Guillaume Lelarge (Dalibo),
 * guillaume.lelarge@dalibo.com
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
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

static char *  stats_recorder_schema = "stats_recorder";
static int     stats_recorder_naptime = 1;
static bool    debug = true;

void	_PG_init(void);

static bool	got_sigterm = false;
static bool	got_sighup = false;

static void
stats_recorder_spi_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	if (debug)
		elog(LOG, "%s, stats_recorder_spi_sigterm", MyBgworkerEntry->bgw_name);

	elog(LOG, "%s shutting down", MyBgworkerEntry->bgw_name);
	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
stats_recorder_spi_sighup(SIGNAL_ARGS)
{
	if (debug)
		elog(LOG, "%s, stats_recorder_spi_sighup", MyBgworkerEntry->bgw_name);

	elog(LOG, "%s reloading configuration", MyBgworkerEntry->bgw_name);
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

static void
initialize_stats_recorder_spi()
{
	int		ret;
	int		ntup;
	bool	isnull;
	StringInfoData	buf;

	if (debug)
		elog(LOG, "%s, initialize_stats_recorder_spi", MyBgworkerEntry->bgw_name);

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT count(*) FROM pg_namespace WHERE nspname = '%s'",
					 stats_recorder_schema);

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	ntup = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));
	if (isnull)
		elog(FATAL, "null result");

	if (ntup == 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE SCHEMA \"%s\" "
						 "CREATE TABLE \"stat_bgwriter\" (dlog timestamp, LIKE pg_stat_bgwriter)",
						 stats_recorder_schema);

		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create my schema");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void
stats_recorder_spi_main(Datum main_arg)
{
	StringInfoData	buf;

	if (debug)
		elog(LOG, "%s, stats_recorder_spi_main", MyBgworkerEntry->bgw_name);

	/* Set up the sigterm/sighup signal functions before unblocking them */
	pqsignal(SIGTERM, stats_recorder_spi_sigterm);
	pqsignal(SIGHUP, stats_recorder_spi_sighup);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	elog(LOG, "%s initialized with schema %s",
		 MyBgworkerEntry->bgw_name, stats_recorder_schema);
	initialize_stats_recorder_spi();

	/*
	 * Quote identifiers passed to us.  Note that this must be done after
	 * initialize_stats_recorder_spi, because that routine assumes the names are not
	 * quoted.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "INSERT INTO %s.stat_bgwriter "
					 "SELECT clock_timestamp(), * FROM pg_stat_bgwriter",
					 quote_identifier(stats_recorder_schema));

	while (!got_sigterm)
	{
		int		ret;
		int		rc;

		elog(LOG, "%s, stats_recorder_spi_main loop, stats_recorder_naptime is %d", MyBgworkerEntry->bgw_name, stats_recorder_naptime);

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   stats_recorder_naptime*1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_INSERT)
			elog(FATAL, "cannot insert into table %s.stat_bgwriter: error code %d",
				 stats_recorder_schema, ret);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	proc_exit(0);
}

/*
 * Entrypoint of this module.
 *
 * We register one worker process.
 */
void
_PG_init(void)
{
	BackgroundWorker	worker;

	if (debug)
		elog(LOG, "stats_recorder, _PG_init");

	/*   
 	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomStringVariable("stats_recorder.schema",
				"Schema in which we'll store the recorded statistics.",
				NULL,
				&stats_recorder_schema,
				"stats_recorder",
				PGC_POSTMASTER,
				0,
				NULL,
				NULL,
				NULL);
	DefineCustomIntVariable("stats_recorder.naptime",
				"Duration between each statistics gathering (in seconds).",
				NULL,
				&stats_recorder_naptime,
				1,
				1,
				INT_MAX,
				PGC_SIGHUP,
				0,
				NULL,
				NULL,
				NULL);

	/* register the worker process */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = stats_recorder_spi_main;
	worker.bgw_main_arg = (Datum) 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", stats_recorder_schema);
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	RegisterBackgroundWorker(&worker);
}
