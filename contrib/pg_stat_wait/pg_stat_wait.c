#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pg_stat_wait.h"
#include "port/atomics.h"
#include "postmaster/autovacuum.h"
#include "storage/spin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/guc.h"
#include "utils/wait.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

/* Global variables */
bool					shmem_initialized = false;
bool					waitsHistoryOn;
int						historySize;
int						historyPeriod;
bool					historySkipLatch;
shm_toc				   *toc = NULL;
CollectorShmqHeader	   *collector_hdr = NULL;
shm_mq				   *collector_mq = NULL;
CurrentWaitEventWrap   *cur_wait_events = NULL;

static int maxProcs;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static wait_event_start_hook_type prev_wait_event_start_hook = NULL;
static wait_event_stop_hook_type prev_wait_event_stop_hook = NULL;

static PGPROC * search_proc(int backendPid);
static TupleDesc get_history_item_tupledesc();
static HeapTuple get_history_item_tuple(HistoryItem *item, TupleDesc tuple_desc);


/*
 * Estimate amount of shared memory needed.
 */
static Size
pgsw_shmem_size(void)
{
	shm_toc_estimator	e;
	Size				size;
	int					nkeys;

	shm_toc_initialize_estimator(&e);

	shm_toc_estimate_chunk(&e, sizeof(CurrentWaitEventWrap) * maxProcs);
	nkeys = 1;

	if (waitsHistoryOn)
	{
		shm_toc_estimate_chunk(&e, sizeof(CollectorShmqHeader));
		shm_toc_estimate_chunk(&e, (Size) COLLECTOR_QUEUE_SIZE);
		nkeys += 2;
	}

	shm_toc_estimate_keys(&e, nkeys);
	size = shm_toc_estimate(&e);

	return size;
}

static void
init_current_wait_event()
{
	int			i;
	instr_time	t;

	INSTR_TIME_SET_CURRENT(t);
	for (i = 0; i < maxProcs; i++)
	{
		CurrentWaitEventWrap   *wrap = &cur_wait_events[i];

		memset(wrap, 0, sizeof(*wrap));
		wrap->data[0].start_time = t;
		wrap->data[1].start_time = t;
	}
}


/*
 * Distribute shared memory.
 */
static void
pgsw_shmem_startup(void)
{
	bool	found;
	Size	segsize = pgsw_shmem_size();
	void   *pgsw;

	pgsw = ShmemInitStruct("pg_stat_wait", segsize, &found);

	if (!found)
	{
		toc = shm_toc_create(PG_STAT_WAIT_MAGIC, pgsw, segsize);

		cur_wait_events = shm_toc_allocate(toc, sizeof(CurrentWaitEventWrap) * maxProcs);
		shm_toc_insert(toc, 0, cur_wait_events);
		init_current_wait_event();

		if (waitsHistoryOn)
		{
			collector_hdr = shm_toc_allocate(toc, sizeof(CollectorShmqHeader));
			shm_toc_insert(toc, 1, collector_hdr);
			collector_mq = shm_toc_allocate(toc, COLLECTOR_QUEUE_SIZE);
			shm_toc_insert(toc, 2, collector_mq);
		}
	}
	else
	{
		toc = shm_toc_attach(PG_STAT_WAIT_MAGIC, pgsw);

		cur_wait_events = shm_toc_lookup(toc, 0);

		if (waitsHistoryOn)
		{
			collector_hdr = shm_toc_lookup(toc, 1);
			collector_mq = shm_toc_lookup(toc, 2);
		}
	}

	shmem_initialized = true;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
}

/*
 * Wait event start hook: put wait information to the shared memory.
 */
static void
pgsw_wait_event_start_hook(uint32 classid, uint32 eventid,
						   uint32 p1, uint32 p2, uint32 p3, uint32 p4, uint32 p5)
{
	CurrentWaitEventWrap   *wrap;
	CurrentWaitEvent	   *event;

	if (!shmem_initialized || !MyProc)
		return;

	wrap = &cur_wait_events[MyProc->pgprocno];
	event = &wrap->data[(wrap->curidx + 1) % 2];

	event->classeventid = (classid << 16) | eventid;
	event->params[0] = p1;
	event->params[1] = p2;
	event->params[2] = p3;
	event->params[3] = p4;
	event->params[4] = p5;
	INSTR_TIME_SET_CURRENT(event->start_time);

	pg_write_barrier();
	wrap->curidx++;

	if (prev_wait_event_start_hook)
		prev_wait_event_start_hook(classid, eventid, p1, p2, p3, p4, p5);
}

/*
 * Wait event stop hook: clear wait information in the shared memory.
 */
static void
pgsw_wait_event_stop_hook(void)
{
	CurrentWaitEventWrap   *wrap;
	CurrentWaitEvent	   *event;

	if (!shmem_initialized || !MyProc)
		return;

	wrap = &cur_wait_events[MyProc->pgprocno];
	event = &wrap->data[(wrap->curidx + 1) % 2];

	event->classeventid = (WAIT_CPU << 16) | WAIT_CPU_BUSY;
	event->params[0] = 0;
	event->params[1] = 0;
	event->params[2] = 0;
	event->params[3] = 0;
	event->params[4] = 0;
	INSTR_TIME_SET_CURRENT(event->start_time);

	pg_write_barrier();
	wrap->curidx++;

	if (prev_wait_event_stop_hook)
		prev_wait_event_stop_hook();
}

/*
 * Check shared memory is initialized. Report an error otherwise.
 */
void
check_shmem(void)
{
	if (!shmem_initialized)
	{
		elog(LOG, "%d", MyProcPid);

		pg_usleep(10*1000*1000);

		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_stat_wait shared memory wasn't initialized yet")));
	}
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("pg_stat_wait.history", "Collect waits history",
			NULL, &waitsHistoryOn, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_stat_wait.history_size",
			"Sets size of waits history.", NULL,
			&historySize, 5000, 100, INT_MAX,
			PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_stat_wait.history_period",
			"Sets period of waits history sampling.", NULL,
			&historyPeriod, 10, 1, INT_MAX,
			PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_stat_wait.history_skip_latch",
			"Skip latch events in waits history", NULL,
			&historySkipLatch, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/* Calculate maximem number of processes */
	maxProcs = MaxConnections + autovacuum_max_workers + 1 +
			   max_worker_processes + NUM_AUXILIARY_PROCS;

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgsw_shmem_startup().
	 */
	RequestAddinShmemSpace(pgsw_shmem_size());

	if (waitsHistoryOn)
		RegisterWaitsCollector();

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgsw_shmem_startup;

	prev_wait_event_start_hook = wait_event_start_hook;
	wait_event_start_hook = pgsw_wait_event_start_hook;

	prev_wait_event_stop_hook = wait_event_stop_hook;
	wait_event_stop_hook = pgsw_wait_event_stop_hook;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
}

static TupleDesc
get_history_item_tupledesc()
{
	int i;
	TupleDesc tupdesc;
	tupdesc = CreateTemplateTupleDesc(HISTORY_COLUMNS_COUNT, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "sample_ts",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "class_id",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "event_id",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "wait_time",
					   INT8OID, -1, 0);

	for (i = 0; i < WAIT_PARAMS_COUNT; i++)
	{
		TupleDescInitEntry(tupdesc, (AttrNumber) (6 + i), "p",
						   INT4OID, -1, 0);
	}

	return BlessTupleDesc(tupdesc);
}

static HeapTuple
get_history_item_tuple(HistoryItem *item, TupleDesc tuple_desc)
{
	int       i;
	HeapTuple tuple;
	Datum     values[HISTORY_COLUMNS_COUNT];
	bool      nulls[HISTORY_COLUMNS_COUNT];

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	/* Values available to all callers */
	values[0] = Int32GetDatum(item->backendPid);
	values[1] = TimestampTzGetDatum(item->ts);
	values[2] = Int32GetDatum(item->classid);
	values[3] = Int32GetDatum(item->eventid);
	values[4] = Int64GetDatum(item->waitTime);

	for (i=0; i < WAIT_PARAMS_COUNT; i++)
		values[5 + i] = Int32GetDatum(item->params[i]);

	tuple = heap_form_tuple(tuple_desc, values, nulls);
	return tuple;
}

PG_FUNCTION_INFO_V1(pg_stat_wait_get_current);
Datum
pg_stat_wait_get_current(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	WaitCurrentContext 	*params;
	HistoryItem 		*currentState;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		WaitCurrentContext 	*params;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		params = (WaitCurrentContext *)palloc0(sizeof(WaitCurrentContext));
		params->ts = GetCurrentTimestamp();

		funcctx->user_fctx = params;
		funcctx->tuple_desc = get_history_item_tupledesc();

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		if (!PG_ARGISNULL(0))
		{
			HistoryItem		item;
			PGPROC		   *proc;

			proc = search_proc(PG_GETARG_UINT32(0));
			ReadCurrentWait(proc, &item);
			params->state = (HistoryItem *)palloc0(sizeof(HistoryItem));
			funcctx->max_calls = 1;
			*params->state = item;
		}
		else
		{
			int					procCount = ProcGlobal->allProcCount,
								i,
								j = 0;
			Timestamp			currentTs = GetCurrentTimestamp();

			params->state = (HistoryItem *) palloc0(sizeof(HistoryItem) * procCount);
			for (i = 0; i < procCount; i++)
			{
				PGPROC *proc = &ProcGlobal->allProcs[i];

				if (proc != NULL && proc->pid != 0)
				{
					ReadCurrentWait(proc, &params->state[j]);
					params->state[j].ts = currentTs;
					j++;
				}
			}
			funcctx->max_calls = j;
		}

		LWLockRelease(ProcArrayLock);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	params = (WaitCurrentContext *)funcctx->user_fctx;
	currentState = NULL;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		HeapTuple tuple;

		tuple = get_history_item_tuple(&params->state[funcctx->call_cntr],
									   funcctx->tuple_desc);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(pg_stat_wait_get_profile);
Datum
pg_stat_wait_get_profile(PG_FUNCTION_ARGS)
{
	WaitProfileContext *params;
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		params = (WaitProfileContext *)palloc0(sizeof(WaitProfileContext));

		if (!PG_ARGISNULL(0))
			params->backendPid = PG_GETARG_UINT32(0);

		if (!PG_ARGISNULL(1))
			params->reset = PG_GETARG_BOOL(1);

		funcctx->user_fctx = params;
#ifdef NOT_USED
		funcctx->max_calls = MaxBackends * WAIT_EVENTS_COUNT;
#endif

		tupdesc = CreateTemplateTupleDesc(5, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "class_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "wait_time",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "wait_count",
						   INT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	params = (WaitProfileContext *)funcctx->user_fctx;
#ifdef NOT_USED
	shmemCells = (BackendWaitCells *)((char *)WaitShmem
			+ sizeof(int) +  /* counter */
			+ MAXALIGN(sizeof(slock_t))); /* lock */

	while (params->backendIdx <= MaxBackends)
	{
		Datum		values[5];
		bool		nulls[5];
		HeapTuple	tuple;
		int count;
		BackendWaitCells bcells, *item;

		if (params->backendIdx == MaxBackends)
		{
			SRF_RETURN_DONE(funcctx);
			break;
		}

		item = &shmemCells[params->backendIdx];

		do
		{
			/* wait until backend is updating this block */
		} while (!pg_atomic_test_set_flag(&item->isBusy));

		memcpy(&bcells, item, sizeof(BackendWaitCells));
		if (params->reset)
		{
			item->cells[params->eventIdx].interval = 0;
			item->cells[params->eventIdx].count = 0;
		}
		pg_atomic_clear_flag(&item->isBusy);

		/* filtering */
		if (bcells.backendPid == 0 ||
				(params->backendPid && params->backendPid != bcells.backendPid))
		{
			params->backendIdx++;
			continue;
		}

		if (params->eventIdx == WAIT_EVENTS_COUNT)
		{
			params->classIdx = 0;
			params->eventIdx = 0;
			params->backendIdx++;
			Assert(params->backendIdx <= MaxBackends);
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		if ((params->classIdx+1) < WAITS_COUNT &&
				(params->eventIdx == WAIT_OFFSETS[params->classIdx+1]))
			params->classIdx++;

		count = bcells.cells[params->eventIdx].count;
		if (count == 0)
		{
			params->eventIdx++;
			continue;
		}

		values[0] = Int32GetDatum(bcells.backendPid);
		values[1] = Int32GetDatum(params->classIdx);
		values[2] = Int32GetDatum(params->eventIdx - WAIT_OFFSETS[params->classIdx]);
		values[3] = Int64GetDatum(bcells.cells[params->eventIdx].interval);
		values[4] = Int32GetDatum(bcells.cells[params->eventIdx].count);
		params->eventIdx += 1;
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
		break;
	}
#endif

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_stat_wait_reset_profile);
Datum
pg_stat_wait_reset_profile(PG_FUNCTION_ARGS)
{
#ifdef NOT_USED
	int i, j;
	BackendWaitCells *cells = (BackendWaitCells *)((char *)WaitShmem
			+ sizeof(int) +  /* counter */
			+ MAXALIGN(sizeof(slock_t))); /* lock */

	for(i = 0; i < MaxBackends; i++)
	{
		BackendWaitCells *item = cells + i;
		do
		{
			/* wait until backend is updating this block */
		} while (!pg_atomic_test_set_flag(&item->isBusy));

		for (j = 0; j < WAIT_EVENTS_COUNT; j++)
		{
			item->cells[j].interval = 0;
			item->cells[j].count = 0;
		}
		pg_atomic_clear_flag(&item->isBusy);
	}
#endif
	PG_RETURN_VOID();
}

static void
initLockTag(LOCKTAG *tag)
{
	tag->locktag_field1 = PG_STAT_WAIT_MAGIC;
	tag->locktag_field2 = 0;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
}

static History *
receive_observations(shm_mq_handle *mqh)
{
	Size len;
	void *data;
	History *result;
	int	count, i;
	shm_mq_result res;

	res = shm_mq_receive(mqh, &len, &data, false);
	if (res != SHM_MQ_SUCCESS)
		elog(ERROR, "Error reading mq.");
	if (len != sizeof(count))
		elog(ERROR, "Invalid message length.");
	memcpy(&count, data, sizeof(count));

	result = (History *)palloc(sizeof(History));
	AllocHistory(result, count);

	for (i = 0; i < count; i++)
	{
		res = shm_mq_receive(mqh, &len, &data, false);
		if (res != SHM_MQ_SUCCESS)
			elog(ERROR, "Error reading mq.");
		if (len != sizeof(HistoryItem))
			elog(ERROR, "Invalid message length.");
		memcpy(&result->items[i], data, sizeof(HistoryItem));
	}

	return result;
}

PG_FUNCTION_INFO_V1(pg_stat_wait_get_history);
Datum
pg_stat_wait_get_history(PG_FUNCTION_ARGS)
{
	History				*observations;
	FuncCallContext		*funcctx;

	check_shmem();

	if (!waitsHistoryOn)
		ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("Waits history turned off")));

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		LOCKTAG			tag;
		shm_mq		   *mq;
		shm_mq_handle  *mqh;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		initLockTag(&tag);
		LockAcquire(&tag, ExclusiveLock, false, false);

		mq = shm_mq_create(collector_mq, COLLECTOR_QUEUE_SIZE);
		collector_hdr->request = HISTORY_REQUEST;

		SetLatch(collector_hdr->latch);

		shm_mq_set_receiver(mq, MyProc);
		mqh = shm_mq_attach(mq, NULL, NULL);

		observations = receive_observations(mqh);
		funcctx->user_fctx = observations;
		funcctx->max_calls = observations->count;

		shm_mq_detach(mq);
		LockRelease(&tag, ExclusiveLock, false);

		funcctx->tuple_desc = get_history_item_tupledesc();
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	observations = (History *)funcctx->user_fctx;

	if (observations->index < observations->count)
	{
		HeapTuple	tuple;
		HistoryItem *observation;

		observation = &observations->items[observations->index];
		tuple = get_history_item_tuple(observation, funcctx->tuple_desc);
		observations->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}

	PG_RETURN_VOID();
}

/*
 * Find PGPROC entry responsible for given pid.
 */
static PGPROC *
search_proc(int pid)
{
	int i;

	if (pid == 0)
		return MyProc;

	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	*proc = &ProcGlobal->allProcs[i];
		if (proc->pid && proc->pid == pid)
		{
			return proc;
		}
	}

	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("backend with pid=%d not found", pid)));
	return NULL;
}

PG_FUNCTION_INFO_V1(pg_start_trace);
Datum
pg_start_trace(PG_FUNCTION_ARGS)
{
#ifdef NOT_USED
	PGPROC *proc;
	char *filename = PG_GETARG_CSTRING(1);

	if (strlen(filename) >= WAIT_TRACE_FN_LEN)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("length of filename limited to %d", (WAIT_TRACE_FN_LEN-1))));

	if (!is_absolute_path(filename))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("path must be absolute")));

	proc = NULL;
	if (PG_ARGISNULL(0))
		proc = MyProc;
	else
		proc = search_proc(PG_GETARG_INT32(0));

	if (proc != NULL)
	{
		if (proc->waits.traceOn)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("trace is already working in backend")));

		strcpy(proc->waits.traceFn, filename);
		proc->waits.traceOn = true;
	}
#endif

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_is_in_trace);
Datum
pg_is_in_trace(PG_FUNCTION_ARGS)
{
#ifdef NOT_USED
	PGPROC *proc = NULL;

	if (PG_ARGISNULL(0))
		proc = MyProc;
	else
		proc = search_proc(PG_GETARG_INT32(0));

	if (proc)
		PG_RETURN_BOOL(proc->waits.traceOn);
#endif

	PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(pg_stop_trace);
Datum
pg_stop_trace(PG_FUNCTION_ARGS)
{
	PGPROC *proc = NULL;
	if (PG_ARGISNULL(0))
		proc = MyProc;
	else
		proc = search_proc(PG_GETARG_INT32(0));

#ifdef NOT_USED
	if (proc != NULL)
	{
		if (!proc->waits.traceOn)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("trace is not started")));

		proc->waits.traceOn = false;
	}
#endif

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_stat_wait_start_wait);

Datum
pg_stat_wait_start_wait(PG_FUNCTION_ARGS)
{
	int classId = PG_GETARG_INT32(0);
	int eventId = PG_GETARG_INT32(1);
	int p1 = PG_GETARG_INT32(2);
	int p2 = PG_GETARG_INT32(3);
	int p3 = PG_GETARG_INT32(4);
	int p4 = PG_GETARG_INT32(5);
	int p5 = PG_GETARG_INT32(6);

	WAIT_START(classId, eventId, p1, p2, p3, p4, p5);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_stat_wait_stop_wait);

Datum
pg_stat_wait_stop_wait(PG_FUNCTION_ARGS)
{
	WAIT_STOP();
	PG_RETURN_VOID();
}
