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

#include "nodes/nodes.h"
#include "fmgr.h"

/* A physical tuple coming from a storage AM scan */
typedef void *StorageTuple;

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

}			StorageAmRoutine;

extern StorageAmRoutine * GetStorageAmRoutine(Oid amhandler);
extern StorageAmRoutine * GetStorageAmRoutineByAmId(Oid amoid);
extern StorageAmRoutine * GetHeapamStorageAmRoutine(void);

#endif							/* STORAGEAMAPI_H */
