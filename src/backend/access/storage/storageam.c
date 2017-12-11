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
#include "utils/rel.h"

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
