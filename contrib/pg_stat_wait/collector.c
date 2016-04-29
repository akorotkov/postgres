/*
 * collector.c
 *		Collector of wait event history.
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_wait/collector.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/s_lock.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait.h"
#include "portability/instr_time.h"

#include "pg_stat_wait.h"

static volatile sig_atomic_t shutdown_requested = false;

static void handle_sigterm(SIGNAL_ARGS);
static void collector_main(Datum main_arg);

/*
 * Register background worker for collecting waits history.
 */
void
RegisterWaitsCollector(void)
{
	BackgroundWorker worker;

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 0;
	worker.bgw_main = collector_main;
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_stat_wait collector");
	worker.bgw_main_arg = (Datum)0;
	RegisterBackgroundWorker(&worker);
}

/*
 * Allocate memory for waits history.
 */
void
AllocHistory(History *observations, int count)
{
	observations->items = (HistoryItem *) palloc0(sizeof(HistoryItem) * count);
	observations->index = 0;
	observations->count = count;
	observations->wraparound = false;
}

/*
 * Reallocate memory for changed number of history items.
 */
static void
ReallocHistory(History *observations, int count)
{
	HistoryItem	   *newitems;
	int				copyCount;
	int				i, j;

	newitems = (HistoryItem *) palloc0(sizeof(HistoryItem) * count);

	if (observations->wraparound)
		copyCount = observations->count;
	else
		copyCount = observations->index;

	copyCount = Min(copyCount, count);

	i = 0;
	j = observations->index;
	while (i < copyCount)
	{
		j--;
		if (j < 0)
			j = observations->count - 1;
		memcpy(&newitems[i], &observations->items[j], sizeof(HistoryItem));
		i++;
	}

	pfree(observations->items);
	observations->items = newitems;

	observations->index = copyCount;
	observations->count = count;
	observations->wraparound = false;
}

/* 
 * Read current wait information for given proc.
 */
void
ReadCurrentWait(PGPROC *proc, HistoryItem *item)
{
	CurrentWaitEvent	   *event;
	instr_time startTime, currentTime;
	uint32					previdx;

	while (true)
	{
		CurrentWaitEventWrap   *wrap;

		wrap = &cur_wait_events[proc->pgprocno];
		previdx = wrap->curidx;

		pg_read_barrier();

		event = &wrap->data[previdx % 2];
		item->backendPid = proc->pid;
		item->classid = event->classeventid >> 16;
		item->eventid = event->classeventid & 0xFFFF;
		memcpy(item->params, event->params, sizeof(event->params));
		startTime = event->start_time;

		pg_read_barrier();

		if (wrap->curidx == previdx)
			break;
	}

	INSTR_TIME_SET_CURRENT(currentTime);
	INSTR_TIME_SUBTRACT(currentTime, startTime);
	item->waitTime = INSTR_TIME_GET_MICROSEC(currentTime);
}

static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	shutdown_requested = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

/*
 * Get next item of history with rotation.
 */
static HistoryItem *
get_next_observation(History *observations)
{
	HistoryItem *result;

	result = &observations->items[observations->index];
	observations->index++;
	if (observations->index >= observations->count)
	{
		observations->index = 0;
		observations->wraparound = true;
	}
	return result;
}

/*
 * Read current waits from backends and write them to history array.
 */
static void
write_waits_history(History *observations, TimestampTz current_ts)
{
	int		i,
			newSize;

	newSize = collector_hdr->historySize;
	if (observations->count != newSize)
		ReallocHistory(observations, newSize);

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		HistoryItem		item,
					   *observation;
		PGPROC		   *proc = &ProcGlobal->allProcs[i];

		ReadCurrentWait(proc, &item);

		if (proc->pid == 0)
			continue;

		if (collector_hdr->historySkipLatch && item.classid == WAIT_LATCH)
			continue;

		item.ts = current_ts;
		observation = get_next_observation(observations);
		*observation = item;
	}
	LWLockRelease(ProcArrayLock);
}

/*
 * Send waits history to shared memory queue.
 */
static void
send_history(History *observations, shm_mq_handle *mqh)
{
	int		count,
			i;

	if (observations->wraparound)
		count = observations->count;
	else
		count = observations->index;

	shm_mq_send(mqh, sizeof(count), &count, false);
	for (i = 0; i < count; i++)
		shm_mq_send(mqh, sizeof(HistoryItem), &observations->items[i], false);
}

/*
 * Main routine of wait history collector.
 */
static void
collector_main(Datum main_arg)
{
	History			observations;
	MemoryContext	old_context,
					collector_context;

	/*
	 * Establish signal handlers.
	 *
	 * We want CHECK_FOR_INTERRUPTS() to kill off this worker process just as
	 * it would a normal user backend.  To make that happen, we establish a
	 * signal handler that is a stripped-down version of die().  We don't have
	 * any equivalent of the backend's command-read loop, where interrupts can
	 * be processed immediately, so make sure ImmediateInterruptOK is turned
	 * off.
	 */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	collector_hdr->latch = &MyProc->procLatch;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_stat_wait collector");
	collector_context = AllocSetContextCreate(TopMemoryContext,
			"pg_stat_wait context",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	old_context = MemoryContextSwitchTo(collector_context);
	AllocHistory(&observations, collector_hdr->historySize);
	MemoryContextSwitchTo(old_context);

	while (1)
	{
		int				rc;
		TimestampTz		current_ts;
		shm_mq_handle  *mqh;

		ResetLatch(&MyProc->procLatch);
		current_ts = GetCurrentTimestamp();
		write_waits_history(&observations, current_ts);

		if (shutdown_requested)
			break;

		rc = WaitLatch(&MyProc->procLatch,
			WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
			collector_hdr->historyPeriod);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);

		if (collector_hdr->request == HISTORY_REQUEST)
		{
			collector_hdr->request = NO_REQUEST;

			shm_mq_set_sender(collector_mq, MyProc);
			mqh = shm_mq_attach(collector_mq, NULL, NULL);
			shm_mq_wait_for_attach(mqh);

			if (shm_mq_get_receiver(collector_mq) != NULL)
				send_history(&observations, mqh);

			shm_mq_detach(collector_mq);
		}
	}

	MemoryContextReset(collector_context);

	/*
	 * We're done.  Explicitly detach the shared memory segment so that we
	 * don't get a resource leak warning at commit time.  This will fire any
	 * on_dsm_detach callbacks we've registered, as well.  Once that's done,
	 * we can go ahead and exit.
	 */
	proc_exit(0);
}
