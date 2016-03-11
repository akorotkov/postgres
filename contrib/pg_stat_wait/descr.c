/*
 * descr.c
 *		Meta information about wait events.
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_wait/descr.c
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/wait.h"

#include "pg_stat_wait.h"

/* Names of wait events */
static const char *waitClassNames[] =
{
	"CPU",
	"LWLocks",
	"Locks",
	"Storage",
	"Latch",
	"Network"
};

static const char *cpuWaitNames[] =
{
	"Busy",
	"MemAllocation"
};

static const char *lwlockTrancheNames[] =
{
	"CLogBuffers",
	"CommitTsBuffers",
	"SubtransBuffers",
	"MXActOffsetBuffers",
	"MXActMemberBuffers",
	"AsyncBuffers",
	"OldSerXIDBuffers",
	"WalInsert",
	"BufferContent",
	"BufferIOInProgress",
	"ReplicationOrigins",
	"ReplicationSlotIOInProgress",
	"Proc",
};

static const char *lockWaitNames[] =
{
	"Relation",
	"RelationExtend",
	"Page",
	"Tuple",
	"Transaction",
	"VirtualTransaction",
	"SpeculativeToken",
	"Object",
	"Userlock",
	"Advisory"
};

static const char *ioWaitNames[] =
{
	"SMGR_READ",
	"SMGR_WRITE",
	"SMGR_FSYNC",
	"SMGR_EXTEND",
	"XLOG_READ",
	"XLOG_WRITE",
	"XLOG_FSYNC",
	"SLRU_READ",
	"SLRU_WRITE",
	"SLRU_FSYNC"
};

static const char *networkWaitNames[] =
{
	"READ",
	"WRITE",
	"SYSLOG"
};

const int numberOfEvents[] = {
	WAIT_CPU_EVENTS_COUNT,
	WAIT_LWLOCKS_COUNT,
	WAIT_LOCKS_COUNT,
	WAIT_IO_EVENTS_COUNT,
	1,
	WAIT_NETWORK_EVENTS_COUNT
};

/*
 * Get name of particular wait event.
 */
static const char *
getWaitEventName(uint32 classid, uint32 eventid)
{
	switch (classid)
	{
		case WAIT_CPU:
			if (eventid >= sizeof(cpuWaitNames)/sizeof(cpuWaitNames[0]))
				elog(ERROR, "Invalid cpu wait event: %u", eventid);
			return cpuWaitNames[eventid];
		case WAIT_LWLOCK:
			if (eventid < NUM_INDIVIDUAL_LWLOCKS)
				return MainLWLockNames[eventid];
			eventid -= NUM_INDIVIDUAL_LWLOCKS;
			if (eventid >= sizeof(lwlockTrancheNames)/sizeof(lwlockTrancheNames[0]))
				elog(ERROR, "Invalid lwlock wait event: %u", eventid);
			return lwlockTrancheNames[eventid];
		case WAIT_LOCK:
			if (eventid >= sizeof(lockWaitNames)/sizeof(lockWaitNames[0]))
				elog(ERROR, "Invalid lock wait event: %u", eventid);
			return lockWaitNames[eventid];
		case WAIT_IO:
			if (eventid >= sizeof(ioWaitNames)/sizeof(ioWaitNames[0]))
				elog(ERROR, "Invalid io wait event: %u", eventid);
			return ioWaitNames[eventid];
		case WAIT_LATCH:
			return "Latch";
		case WAIT_NETWORK:
			if (eventid >= sizeof(networkWaitNames)/sizeof(networkWaitNames[0]))
				elog(ERROR, "Invalid network wait event: %u", eventid);
			return networkWaitNames[eventid];
		default:
			elog(ERROR, "Invalid wait event class: %u", classid);
	}
	/* Keep compiler quiet */
	return NULL;
}

PG_FUNCTION_INFO_V1(pg_wait_class_list);
Datum
pg_wait_class_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = WAIT_CLASSES_COUNT;

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "class_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "name",
						   CSTRINGOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[2];
		bool		nulls[2];
		HeapTuple	tuple;
		int idx;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		idx = funcctx->call_cntr;
		values[0] = Int32GetDatum(idx);
		values[1] = CStringGetDatum(waitClassNames[idx]);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(pg_wait_event_list);
Datum
pg_wait_event_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	WaitEventContext *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		/* 4 arrays length and WAIT_LATCH + WAIT_CPU */
		funcctx->max_calls = WAIT_CPU_EVENTS_COUNT + 1 + WAIT_LWLOCKS_COUNT + WAIT_LOCKS_COUNT +
								 WAIT_IO_EVENTS_COUNT + WAIT_NETWORK_EVENTS_COUNT;

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "class_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "event_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "name",
						   CSTRINGOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = palloc0(sizeof(WaitEventContext));

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	ctx = (WaitEventContext *)funcctx->user_fctx;

	if (ctx->class_idx < WAIT_CLASSES_COUNT)
	{
		/* for each row */
		Datum		values[3];
		bool		nulls[3];
		HeapTuple	tuple;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(ctx->class_idx);
		values[1] = Int32GetDatum(ctx->event_idx);

		if (ctx->class_idx < WAIT_CLASSES_COUNT)
		{
			values[2] = CStringGetDatum(getWaitEventName(ctx->class_idx, ctx->event_idx));
			ctx->event_idx++;
			if (ctx->event_idx >= numberOfEvents[ctx->class_idx])
			{
				ctx->event_idx = 0;
				ctx->class_idx++;
			}
		}
		else
		{
			elog(ERROR, "Wrong wait class");
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}
