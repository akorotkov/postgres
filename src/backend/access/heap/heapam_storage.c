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

#include "access/storageamapi.h"
#include "utils/builtins.h"


Datum
heapam_storage_handler(PG_FUNCTION_ARGS)
{
	StorageAmRoutine *amroutine = makeNode(StorageAmRoutine);

	PG_RETURN_POINTER(amroutine);
}
