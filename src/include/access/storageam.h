/*-------------------------------------------------------------------------
 *
 * storageam.h
 *	  POSTGRES storage access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/storageam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGEAM_H
#define STORAGEAM_H

#include "access/heapam.h"
#include "access/storage_common.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

typedef union tuple_data
{
	TransactionId xid;
	CommandId	cid;
	ItemPointerData tid;
}			tuple_data;

typedef enum tuple_data_flags
{
	XMIN = 0,
	UPDATED_XID,
	CMIN,
	TID,
	CTID
}			tuple_data_flags;

/* Function pointer to let the index tuple insert from storage am */
typedef List *(*InsertIndexTuples) (TupleTableSlot *slot, EState *estate, bool noDupErr,
									bool *specConflict, List *arbiterIndexes);


extern StorageScanDesc storage_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan);
extern ParallelHeapScanDesc storageam_get_parallelheapscandesc(StorageScanDesc sscan);
extern HeapPageScanDesc storageam_get_heappagescandesc(StorageScanDesc sscan);
extern void storage_setscanlimits(StorageScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks);
extern StorageScanDesc storage_beginscan(Relation relation, Snapshot snapshot,
										 int nkeys, ScanKey key);
extern StorageScanDesc storage_beginscan_catalog(Relation relation, int nkeys, ScanKey key);
extern StorageScanDesc storage_beginscan_strat(Relation relation, Snapshot snapshot,
											   int nkeys, ScanKey key,
											   bool allow_strat, bool allow_sync);
extern StorageScanDesc storage_beginscan_bm(Relation relation, Snapshot snapshot,
											int nkeys, ScanKey key);
extern StorageScanDesc storage_beginscan_sampling(Relation relation, Snapshot snapshot,
												  int nkeys, ScanKey key,
												  bool allow_strat, bool allow_sync, bool allow_pagemode);

extern void storage_endscan(StorageScanDesc scan);
extern void storage_rescan(StorageScanDesc scan, ScanKey key);
extern void storage_rescan_set_params(StorageScanDesc scan, ScanKey key,
						  bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void storage_update_snapshot(StorageScanDesc scan, Snapshot snapshot);

extern StorageTuple storage_getnext(StorageScanDesc sscan, ScanDirection direction);
extern TupleTableSlot *storage_getnextslot(StorageScanDesc sscan, ScanDirection direction, TupleTableSlot *slot);
extern StorageTuple storage_fetch_tuple_from_offset(StorageScanDesc sscan, BlockNumber blkno, OffsetNumber offset);

extern void storage_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid);

extern bool storage_fetch(Relation relation,
			  ItemPointer tid,
			  Snapshot snapshot,
			  StorageTuple * stuple,
			  Buffer *userbuf,
			  bool keep_buf,
			  Relation stats_relation);

extern bool storage_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
						  Snapshot snapshot, HeapTuple heapTuple,
						  bool *all_dead, bool first_call);

extern bool storage_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot,
				   bool *all_dead);

extern HTSU_Result storage_lock_tuple(Relation relation, ItemPointer tid, StorageTuple * stuple,
				   CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
				   bool follow_updates,
				   Buffer *buffer, HeapUpdateFailureData *hufd);

extern Oid storage_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			   int options, BulkInsertState bistate, InsertIndexTuples IndexFunc,
			   EState *estate, List *arbiterIndexes, List **recheckIndexes);

extern HTSU_Result storage_delete(Relation relation, ItemPointer tid, CommandId cid,
			   Snapshot crosscheck, bool wait,
			   HeapUpdateFailureData *hufd);

extern HTSU_Result storage_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
			   EState *estate, CommandId cid, Snapshot crosscheck, bool wait,
			   HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
			   InsertIndexTuples IndexFunc, List **recheckIndexes);

extern void storage_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
					 CommandId cid, int options, BulkInsertState bistate);

extern void storage_abort_speculative(Relation relation, TupleTableSlot *slot);

extern tuple_data storage_tuple_get_data(Relation relation, StorageTuple tuple, tuple_data_flags flags);

extern StorageTuple storage_tuple_by_datum(Relation relation, Datum data, Oid tableoid);

extern void storage_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid);

extern void storage_sync(Relation rel);

#endif
