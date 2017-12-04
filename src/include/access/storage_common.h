/*-------------------------------------------------------------------------
 *
 * storage_common.h
 *	  POSTGRES storage access method definitions shared across
 *	  all pluggable storage methods and server.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/storage_common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_COMMON_H
#define STORAGE_COMMON_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "executor/tuptable.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"

/* A physical tuple coming from a storage AM scan */
typedef void *StorageTuple;

/*
 * slot storage routine functions
 */
typedef void (*SlotStoreTuple_function) (TupleTableSlot *slot,
										 StorageTuple tuple,
										 bool shouldFree,
										 bool minumumtuple);
typedef void (*SlotClearTuple_function) (TupleTableSlot *slot);
typedef Datum (*SlotGetattr_function) (TupleTableSlot *slot,
									   int attnum, bool *isnull);
typedef void (*SlotVirtualizeTuple_function) (TupleTableSlot *slot, int16 upto);

typedef HeapTuple (*SlotGetTuple_function) (TupleTableSlot *slot, bool palloc_copy);
typedef MinimalTuple (*SlotGetMinTuple_function) (TupleTableSlot *slot, bool palloc_copy);

typedef void (*SlotUpdateTableoid_function) (TupleTableSlot *slot, Oid tableoid);

typedef void (*SpeculativeAbort_function) (Relation rel,
										   TupleTableSlot *slot);

typedef struct StorageSlotAmRoutine
{
	/* Operations on TupleTableSlot */
	SlotStoreTuple_function slot_store_tuple;
	SlotVirtualizeTuple_function slot_virtualize_tuple;
	SlotClearTuple_function slot_clear_tuple;
	SlotGetattr_function slot_getattr;
	SlotGetTuple_function slot_tuple;
	SlotGetMinTuple_function slot_min_tuple;
	SlotUpdateTableoid_function slot_update_tableoid;
}			StorageSlotAmRoutine;

typedef StorageSlotAmRoutine * (*slot_storageam_hook) (void);

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum
{
	HEAPTUPLE_DEAD,				/* tuple is dead and deletable */
	HEAPTUPLE_LIVE,				/* tuple is live (committed, no deleter) */
	HEAPTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	HEAPTUPLE_INSERT_IN_PROGRESS,	/* inserting xact is still in progress */
	HEAPTUPLE_DELETE_IN_PROGRESS	/* deleting xact is still in progress */
} HTSV_Result;


/* in storage/storage_common.c */
extern void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid);
extern bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple);
extern bool HeapTupleIsSurelyDead(HeapTuple htup, TransactionId OldestXmin);
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);
extern StorageSlotAmRoutine * heapam_storage_slot_handler(void);

/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since pre-9.0 VACUUM FULL always used
 * synchronous commits and didn't move tuples that weren't previously
 * hinted.  (This is not known by this subroutine, but is applied by its
 * callers.)  Note: old-style VACUUM FULL is gone, but we have to keep this
 * module's support for MOVED_OFF/MOVED_IN flag bits for as long as we
 * support in-place update from pre-9.0 databases.
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void
SetHintBits(HeapTupleHeader tuple, Buffer buffer,
			uint16 infomask, TransactionId xid)
{
	if (TransactionIdIsValid(xid))
	{
		/* NB: xid must be known committed here! */
		XLogRecPtr	commitLSN = TransactionIdGetCommitLSN(xid);

		if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) &&
			BufferGetLSNAtomic(buffer) < commitLSN)
		{
			/* not flushed and no LSN interlock, so don't set hint */
			return;
		}
	}

	tuple->t_infomask |= infomask;
	MarkBufferDirtyHint(buffer, true);
}

#endif							/* STORAGE_COMMON_H */
