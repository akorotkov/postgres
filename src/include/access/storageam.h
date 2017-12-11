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
#include "access/storageamapi.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

extern bool storage_fetch(Relation relation,
			  ItemPointer tid,
			  Snapshot snapshot,
			  StorageTuple * stuple,
			  Buffer *userbuf,
			  bool keep_buf,
			  Relation stats_relation);

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
