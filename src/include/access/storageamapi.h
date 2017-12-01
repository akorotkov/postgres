/*---------------------------------------------------------------------
 *
 * storageamapi.h
 *		API for Postgres storage access methods
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/include/access/storageamapi.h
 *---------------------------------------------------------------------
 */
#ifndef STORAGEAMAPI_H
#define STORAGEAMAPI_H

#include "access/storage_common.h"
#include "nodes/nodes.h"
#include "utils/snapshot.h"
#include "fmgr.h"

/* A physical tuple coming from a storage AM scan */
typedef void *StorageTuple;

typedef bool (*SnapshotSatisfies_function) (StorageTuple htup, Snapshot snapshot, Buffer buffer);
typedef HTSU_Result (*SnapshotSatisfiesUpdate_function) (StorageTuple htup, CommandId curcid, Buffer buffer);
typedef HTSV_Result (*SnapshotSatisfiesVacuum_function) (StorageTuple htup, TransactionId OldestXmin, Buffer buffer);



/*
 * API struct for a storage AM.  Note this must be stored in a single palloc'd
 * chunk of memory.
 *
 * XXX currently all functions are together in a single struct.  Would it be
 * worthwhile to split the slot-accessor functions to a different struct?
 * That way, MinimalTuple could be handled without a complete StorageAmRoutine
 * for them -- it'd only have a few functions in TupleTableSlotAmRoutine or so.
 */
typedef struct StorageAmRoutine
{
	NodeTag		type;

	SnapshotSatisfies_function snapshot_satisfies;
	SnapshotSatisfiesUpdate_function snapshot_satisfiesUpdate;	/* HeapTupleSatisfiesUpdate */
	SnapshotSatisfiesVacuum_function snapshot_satisfiesVacuum;	/* HeapTupleSatisfiesVacuum */

}			StorageAmRoutine;

extern StorageAmRoutine * GetStorageAmRoutine(Oid amhandler);
extern StorageAmRoutine * GetStorageAmRoutineByAmId(Oid amoid);
extern StorageAmRoutine * GetHeapamStorageAmRoutine(void);

#endif							/* STORAGEAMAPI_H */
