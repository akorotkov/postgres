/*-------------------------------------------------------------------------
 *
 * heapam_storage.c
 *	  heap storage access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_storage.c
 *
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/storageamapi.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/rel.h"

extern bool HeapTupleSatisfies(StorageTuple stup, Snapshot snapshot, Buffer buffer);
extern HTSU_Result HeapTupleSatisfiesUpdate(StorageTuple stup, CommandId curcid,
						 Buffer buffer);
extern HTSV_Result HeapTupleSatisfiesVacuum(StorageTuple stup, TransactionId OldestXmin,
						 Buffer buffer);

/* ----------------------------------------------------------------
 *				storage AM support routines for heapam
 * ----------------------------------------------------------------
 */

static bool
heapam_fetch(Relation relation,
			 ItemPointer tid,
			 Snapshot snapshot,
			 StorageTuple * stuple,
			 Buffer *userbuf,
			 bool keep_buf,
			 Relation stats_relation)
{
	HeapTupleData tuple;

	*stuple = NULL;
	if (heap_fetch(relation, tid, snapshot, &tuple, userbuf, keep_buf, stats_relation))
	{
		*stuple = heap_copytuple(&tuple);
		return true;
	}

	return false;
}

/*
 * Insert a heap tuple from a slot, which may contain an OID and speculative
 * insertion token.
 */
static Oid
heapam_heap_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   int options, BulkInsertState bistate, InsertIndexTuples IndexFunc,
				   EState *estate, List *arbiterIndexes, List **recheckIndexes)
{
	Oid			oid;
	HeapTuple	tuple = NULL;

	if (slot->tts_storage)
	{
		HeapamTuple *htuple = slot->tts_storage;

		tuple = htuple->hst_heaptuple;

		if (relation->rd_rel->relhasoids)
			HeapTupleSetOid(tuple, InvalidOid);
	}
	else
	{
		/*
		 * Obtain the physical tuple to insert, building from the slot values.
		 * XXX: maybe the slot already contains a physical tuple in the right
		 * format?  In fact, if the slot isn't fully deformed, this is
		 * completely bogus ...
		 */
		tuple = heap_form_tuple(slot->tts_tupleDescriptor,
								slot->tts_values,
								slot->tts_isnull);
	}

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	/* Set the speculative insertion token, if the slot has one */
	if ((options & HEAP_INSERT_SPECULATIVE) && slot->tts_speculativeToken)
		HeapTupleHeaderSetSpeculativeToken(tuple->t_data, slot->tts_speculativeToken);

	/* Perform the insertion, and copy the resulting ItemPointer */
	oid = heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (slot->tts_storage == NULL)
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);

	if ((estate != NULL) && (estate->es_result_relation_info->ri_NumIndices > 0))
	{
		Assert(IndexFunc != NULL);

		if (options & HEAP_INSERT_SPECULATIVE)
		{
			bool		specConflict = false;

			*recheckIndexes = (IndexFunc) (slot, estate, true,
										   &specConflict,
										   arbiterIndexes);

			/* adjust the tuple's state accordingly */
			if (!specConflict)
				heap_finish_speculative(relation, slot);
			else
			{
				heap_abort_speculative(relation, slot);
				slot->tts_specConflict = true;
			}
		}
		else
		{
			*recheckIndexes = (IndexFunc) (slot, estate, false,
										   NULL, arbiterIndexes);
		}
	}

	return oid;
}

static HTSU_Result
heapam_heap_delete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot crosscheck, bool wait,
				   HeapUpdateFailureData *hufd)
{
	return heap_delete(relation, tid, cid, crosscheck, wait, hufd);
}

static HTSU_Result
heapam_heap_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
				   EState *estate, CommandId cid, Snapshot crosscheck,
				   bool wait, HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
				   InsertIndexTuples IndexFunc, List **recheckIndexes)
{
	HeapTuple	tuple;
	HTSU_Result result;

	if (slot->tts_storage)
	{
		HeapamTuple *htuple = slot->tts_storage;

		tuple = htuple->hst_heaptuple;
	}
	else
	{
		tuple = heap_form_tuple(slot->tts_tupleDescriptor,
								slot->tts_values,
								slot->tts_isnull);
	}

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	result = heap_update(relation, otid, tuple, cid, crosscheck, wait,
						 hufd, lockmode);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (slot->tts_storage == NULL)
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);

	/*
	 * Note: instead of having to update the old index tuples associated with
	 * the heap tuple, all we do is form and insert new index tuples. This is
	 * because UPDATEs are actually DELETEs and INSERTs, and index tuple
	 * deletion is done later by VACUUM (see notes in ExecDelete). All we do
	 * here is insert new index tuples.  -cim 9/27/89
	 */

	/*
	 * insert index entries for tuple
	 *
	 * Note: heap_update returns the tid (location) of the new tuple in the
	 * t_self field.
	 *
	 * If it's a HOT update, we mustn't insert new index entries.
	 */
	if ((result == HeapTupleMayBeUpdated) &&
		((estate != NULL) && (estate->es_result_relation_info->ri_NumIndices > 0)) &&
		(!HeapTupleIsHeapOnly(tuple)))
		*recheckIndexes = (IndexFunc) (slot, estate, false, NULL, NIL);

	return result;
}

static tuple_data
heapam_get_tuple_data(StorageTuple tuple, tuple_data_flags flags)
{
	tuple_data	result;

	switch (flags)
	{
		case XMIN:
			result.xid = HeapTupleHeaderGetXmin(((HeapTuple) tuple)->t_data);
			break;
		case UPDATED_XID:
			result.xid = HeapTupleHeaderGetUpdateXid(((HeapTuple) tuple)->t_data);
			break;
		case CMIN:
			result.cid = HeapTupleHeaderGetCmin(((HeapTuple) tuple)->t_data);
			break;
		case TID:
			result.tid = ((HeapTuple) tuple)->t_self;
			break;
		case CTID:
			result.tid = ((HeapTuple) tuple)->t_data->t_ctid;
			break;
		default:
			Assert(0);
			break;
	}

	return result;
}

static StorageTuple
heapam_form_tuple_by_datum(Datum data, Oid tableoid)
{
	return heap_form_tuple_by_datum(data, tableoid);
}

static ParallelHeapScanDesc
heapam_get_parallelheapscandesc(StorageScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	return scan->rs_parallel;
}

static HeapPageScanDesc
heapam_get_heappagescandesc(StorageScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	return &scan->rs_pagescan;
}

static StorageTuple
heapam_fetch_tuple_from_offset(StorageScanDesc sscan, BlockNumber blkno, OffsetNumber offset)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	Page		dp;
	ItemId		lp;

	dp = (Page) BufferGetPage(scan->rs_scan.rs_cbuf);
	lp = PageGetItemId(dp, offset);
	Assert(ItemIdIsNormal(lp));

	scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
	scan->rs_ctup.t_len = ItemIdGetLength(lp);
	scan->rs_ctup.t_tableOid = scan->rs_scan.rs_rd->rd_id;
	ItemPointerSet(&scan->rs_ctup.t_self, blkno, offset);

	pgstat_count_heap_fetch(scan->rs_scan.rs_rd);

	return &(scan->rs_ctup);
}


Datum
heapam_storage_handler(PG_FUNCTION_ARGS)
{
	StorageAmRoutine *amroutine = makeNode(StorageAmRoutine);

	amroutine->snapshot_satisfies = HeapTupleSatisfies;

	amroutine->snapshot_satisfiesUpdate = HeapTupleSatisfiesUpdate;
	amroutine->snapshot_satisfiesVacuum = HeapTupleSatisfiesVacuum;

	amroutine->slot_storageam = heapam_storage_slot_handler;

	amroutine->scan_begin = heap_beginscan;
	amroutine->scansetlimits = heap_setscanlimits;
	amroutine->scan_getnext = heap_getnext;
	amroutine->scan_getnextslot = heap_getnextslot;
	amroutine->scan_end = heap_endscan;
	amroutine->scan_rescan = heap_rescan;
	amroutine->scan_update_snapshot = heap_update_snapshot;
	amroutine->hot_search_buffer = heap_hot_search_buffer;
	amroutine->scan_fetch_tuple_from_offset = heapam_fetch_tuple_from_offset;

	/*
	 * The following routine needs to be provided when the storage support
	 * parallel sequential scan
	 */
	amroutine->scan_get_parallelheapscandesc = heapam_get_parallelheapscandesc;

	/*
	 * The following routine needs to be provided when the storage support
	 * BitmapHeap and Sample Scans
	 */
	amroutine->scan_get_heappagescandesc = heapam_get_heappagescandesc;

	amroutine->tuple_fetch = heapam_fetch;
	amroutine->tuple_insert = heapam_heap_insert;
	amroutine->tuple_delete = heapam_heap_delete;
	amroutine->tuple_update = heapam_heap_update;
	amroutine->tuple_lock = heap_lock_tuple;
	amroutine->multi_insert = heap_multi_insert;

	amroutine->get_tuple_data = heapam_get_tuple_data;
	amroutine->tuple_from_datum = heapam_form_tuple_by_datum;
	amroutine->tuple_get_latest_tid = heap_get_latest_tid;
	amroutine->speculative_abort = heap_abort_speculative;
	amroutine->relation_sync = heap_sync;

	PG_RETURN_POINTER(amroutine);
}
