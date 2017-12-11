/*-------------------------------------------------------------------------
 *
 * storageam.c
 *	  storage access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/storage/storageam.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/storageam.h"
#include "access/storageamapi.h"
#include "access/relscan.h"
#include "utils/rel.h"
#include "utils/tqual.h"

/*
 *	storage_fetch		- retrieve tuple with given tid
 */
bool
storage_fetch(Relation relation,
			  ItemPointer tid,
			  Snapshot snapshot,
			  StorageTuple * stuple,
			  Buffer *userbuf,
			  bool keep_buf,
			  Relation stats_relation)
{
	return relation->rd_stamroutine->tuple_fetch(relation, tid, snapshot, stuple,
												 userbuf, keep_buf, stats_relation);
}


/*
 *	storage_lock_tuple - lock a tuple in shared or exclusive mode
 */
HTSU_Result
storage_lock_tuple(Relation relation, ItemPointer tid, StorageTuple * stuple,
				   CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
				   bool follow_updates, Buffer *buffer, HeapUpdateFailureData *hufd)
{
	return relation->rd_stamroutine->tuple_lock(relation, tid, stuple,
												cid, mode, wait_policy,
												follow_updates, buffer, hufd);
}

/* ----------------
 *		heap_beginscan_parallel - join a parallel scan
 *
 *		Caller must hold a suitable lock on the correct relation.
 * ----------------
 */
StorageScanDesc
storage_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan)
{
	Snapshot	snapshot;

	Assert(RelationGetRelid(relation) == parallel_scan->phs_relid);
	snapshot = RestoreSnapshot(parallel_scan->phs_snapshot_data);
	RegisterSnapshot(snapshot);

	return relation->rd_stamroutine->scan_begin(relation, snapshot, 0, NULL, parallel_scan,
												true, true, true, false, false, true);
}

ParallelHeapScanDesc
storageam_get_parallelheapscandesc(StorageScanDesc sscan)
{
	return sscan->rs_rd->rd_stamroutine->scan_get_parallelheapscandesc(sscan);
}

HeapPageScanDesc
storageam_get_heappagescandesc(StorageScanDesc sscan)
{
	/*
	 * Planner should have already validated whether the current storage
	 * supports Page scans are not? This function will be called only from
	 * Bitmap Heap scan and sample scan
	 */
	Assert(sscan->rs_rd->rd_stamroutine->scan_get_heappagescandesc != NULL);

	return sscan->rs_rd->rd_stamroutine->scan_get_heappagescandesc(sscan);
}

/*
 * heap_setscanlimits - restrict range of a heapscan
 *
 * startBlk is the page to start at
 * numBlks is number of pages to scan (InvalidBlockNumber means "all")
 */
void
storage_setscanlimits(StorageScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks)
{
	sscan->rs_rd->rd_stamroutine->scansetlimits(sscan, startBlk, numBlks);
}


/* ----------------
 *		heap_beginscan	- begin relation scan
 *
 * heap_beginscan is the "standard" case.
 *
 * heap_beginscan_catalog differs in setting up its own temporary snapshot.
 *
 * heap_beginscan_strat offers an extended API that lets the caller control
 * whether a nondefault buffer access strategy can be used, and whether
 * syncscan can be chosen (possibly resulting in the scan not starting from
 * block zero).  Both of these default to true with plain heap_beginscan.
 *
 * heap_beginscan_bm is an alternative entry point for setting up a
 * HeapScanDesc for a bitmap heap scan.  Although that scan technology is
 * really quite unlike a standard seqscan, there is just enough commonality
 * to make it worth using the same data structure.
 *
 * heap_beginscan_sampling is an alternative entry point for setting up a
 * HeapScanDesc for a TABLESAMPLE scan.  As with bitmap scans, it's worth
 * using the same data structure although the behavior is rather different.
 * In addition to the options offered by heap_beginscan_strat, this call
 * also allows control of whether page-mode visibility checking is used.
 * ----------------
 */
StorageScanDesc
storage_beginscan(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												true, true, true, false, false, false);
}

StorageScanDesc
storage_beginscan_catalog(Relation relation, int nkeys, ScanKey key)
{
	Oid			relid = RelationGetRelid(relation);
	Snapshot	snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));

	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												true, true, true, false, false, true);
}

StorageScanDesc
storage_beginscan_strat(Relation relation, Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												allow_strat, allow_sync, true,
												false, false, false);
}

StorageScanDesc
storage_beginscan_bm(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												false, false, true, true, false, false);
}

StorageScanDesc
storage_beginscan_sampling(Relation relation, Snapshot snapshot,
						   int nkeys, ScanKey key,
						   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
												allow_strat, allow_sync, allow_pagemode,
												false, true, false);
}

/* ----------------
 *		heap_rescan		- restart a relation scan
 * ----------------
 */
void
storage_rescan(StorageScanDesc scan,
			   ScanKey key)
{
	scan->rs_rd->rd_stamroutine->scan_rescan(scan, key, false, false, false, false);
}

/* ----------------
 *		heap_rescan_set_params	- restart a relation scan after changing params
 *
 * This call allows changing the buffer strategy, syncscan, and pagemode
 * options before starting a fresh scan.  Note that although the actual use
 * of syncscan might change (effectively, enabling or disabling reporting),
 * the previously selected startblock will be kept.
 * ----------------
 */
void
storage_rescan_set_params(StorageScanDesc scan, ScanKey key,
						  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	scan->rs_rd->rd_stamroutine->scan_rescan(scan, key, true,
											 allow_strat, allow_sync, (allow_pagemode && IsMVCCSnapshot(scan->rs_snapshot)));
}

/* ----------------
 *		heap_endscan	- end relation scan
 *
 *		See how to integrate with index scans.
 *		Check handling if reldesc caching.
 * ----------------
 */
void
storage_endscan(StorageScanDesc scan)
{
	scan->rs_rd->rd_stamroutine->scan_end(scan);
}


/* ----------------
 *		heap_update_snapshot
 *
 *		Update snapshot info in heap scan descriptor.
 * ----------------
 */
void
storage_update_snapshot(StorageScanDesc scan, Snapshot snapshot)
{
	scan->rs_rd->rd_stamroutine->scan_update_snapshot(scan, snapshot);
}

StorageTuple
storage_getnext(StorageScanDesc sscan, ScanDirection direction)
{
	return sscan->rs_rd->rd_stamroutine->scan_getnext(sscan, direction);
}

TupleTableSlot *
storage_getnextslot(StorageScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	return sscan->rs_rd->rd_stamroutine->scan_getnextslot(sscan, direction, slot);
}

StorageTuple
storage_fetch_tuple_from_offset(StorageScanDesc sscan, BlockNumber blkno, OffsetNumber offset)
{
	return sscan->rs_rd->rd_stamroutine->scan_fetch_tuple_from_offset(sscan, blkno, offset);
}

/*
 * Insert a tuple from a slot into storage AM routine
 */
Oid
storage_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			   int options, BulkInsertState bistate, InsertIndexTuples IndexFunc,
			   EState *estate, List *arbiterIndexes, List **recheckIndexes)
{
	return relation->rd_stamroutine->tuple_insert(relation, slot, cid, options,
												  bistate, IndexFunc, estate,
												  arbiterIndexes, recheckIndexes);
}

/*
 * Delete a tuple from tid using storage AM routine
 */
HTSU_Result
storage_delete(Relation relation, ItemPointer tid, CommandId cid,
			   Snapshot crosscheck, bool wait,
			   HeapUpdateFailureData *hufd)
{
	return relation->rd_stamroutine->tuple_delete(relation, tid, cid,
												  crosscheck, wait, hufd);
}

/*
 * update a tuple from tid using storage AM routine
 */
HTSU_Result
storage_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
			   EState *estate, CommandId cid, Snapshot crosscheck, bool wait,
			   HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
			   InsertIndexTuples IndexFunc, List **recheckIndexes)
{
	return relation->rd_stamroutine->tuple_update(relation, otid, slot, estate,
												  cid, crosscheck, wait, hufd,
												  lockmode, IndexFunc, recheckIndexes);
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return true.  If no match, return false without modifying *tid.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set true if all members of the HOT chain
 * are vacuumable, false if not.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.  Also unlike
 * heap_fetch, we do not report any pgstats count; caller may do so if wanted.
 */
bool
storage_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
						  Snapshot snapshot, HeapTuple heapTuple,
						  bool *all_dead, bool first_call)
{
	return relation->rd_stamroutine->hot_search_buffer(tid, relation, buffer,
													   snapshot, heapTuple, all_dead, first_call);
}

/*
 *	heap_hot_search		- search HOT chain for tuple satisfying snapshot
 *
 * This has the same API as heap_hot_search_buffer, except that the caller
 * does not provide the buffer containing the page, rather we access it
 * locally.
 */
bool
storage_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot,
				   bool *all_dead)
{
	bool		result;
	Buffer		buffer;
	HeapTupleData heapTuple;

	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	result = relation->rd_stamroutine->hot_search_buffer(tid, relation, buffer,
														 snapshot, &heapTuple, all_dead, true);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	return result;
}

/*
 *	storage_multi_insert	- insert multiple tuple into a storage
 */
void
storage_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
					 CommandId cid, int options, BulkInsertState bistate)
{
	relation->rd_stamroutine->multi_insert(relation, tuples, ntuples,
										   cid, options, bistate);
}

/*
 *	storage_abort_speculative - kill a speculatively inserted tuple
 */
void
storage_abort_speculative(Relation relation, TupleTableSlot *slot)
{
	relation->rd_stamroutine->speculative_abort(relation, slot);
}

tuple_data
storage_tuple_get_data(Relation relation, StorageTuple tuple, tuple_data_flags flags)
{
	return relation->rd_stamroutine->get_tuple_data(tuple, flags);
}

StorageTuple
storage_tuple_by_datum(Relation relation, Datum data, Oid tableoid)
{
	if (relation)
		return relation->rd_stamroutine->tuple_from_datum(data, tableoid);
	else
		return heap_form_tuple_by_datum(data, tableoid);
}

void
storage_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid)
{
	relation->rd_stamroutine->tuple_get_latest_tid(relation, snapshot, tid);
}

/*
 *	storage_sync		- sync a heap, for use when no WAL has been written
 */
void
storage_sync(Relation rel)
{
	rel->rd_stamroutine->relation_sync(rel);
}
