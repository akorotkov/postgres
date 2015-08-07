#ifndef _WAIT_H_
#define _WAIT_H_

#include <time.h>
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "portability/instr_time.h"

/*
   Waits logging

   There are two main functions: StartWait and StopWait.
   StartWait is called at the beginning, StopWait at the end of wait.
   Every wait has it's own parameters. Parameters count must be equal with
   WAIT_PARAMS_COUNT. Every backend contains sum of intervals and count for each wait.
   GUC `waits_flush_period` regulate how often data from backend will be flushed
   to shared memory and will be visible in views.
   In shared memory we allocate space that is enough to hold data for all backends.
   When process is starting it reservers block in shared memory,
   when dies it marks that the block is free.

   Monitored waits by now:

   1) Heavy-weight locks (lock.c)
   2) LW-Locks (lwlock.c)
   3) IO read-write (md.c)
   4) Network (be-secure.c)
   5) Latches (pg_latch.c)
 */

enum WaitClasses
{
	WAIT_LWLOCK = 1,
	WAIT_LOCK,
	WAIT_IO,
	WAIT_LATCH,
	WAIT_NETWORK,
	WAIT_ALLOC,
	/* Last item as count */
	WAITS_COUNT
} WaitClasses;

enum WaitIOEvents
{
	WAIT_SMGR_READ,
	WAIT_SMGR_WRITE,
	WAIT_SMGR_FSYNC,
	WAIT_XLOG_READ,
	WAIT_XLOG_WRITE,
	WAIT_XLOG_FSYNC,
	WAIT_SLRU_READ,
	WAIT_SLRU_WRITE,
	WAIT_SLRU_FSYNC,
	/* Last item as count */
	WAIT_IO_EVENTS_COUNT
} WaitIOEvents;

enum WaitNetworkEvents
{
	WAIT_NETWORK_READ,
	WAIT_NETWORK_WRITE,
	WAIT_SYSLOG,
	/* last item as count */
	WAIT_NETWORK_EVENTS_COUNT
} WaitNetworkEvents;

#define WAIT_LWLOCKS_COUNT         NUM_LWLOCK_TRANCHES
#define WAIT_LOCKS_COUNT           (LOCKTAG_LAST_TYPE + 1)

/* Waits in arrays in backends and in shared memory located by offsets */
#define WAIT_LWLOCKS_OFFSET  0
#define WAIT_LOCKS_OFFSET    (WAIT_LWLOCKS_OFFSET + WAIT_LWLOCKS_COUNT)
#define WAIT_IO_OFFSET       (WAIT_LOCKS_OFFSET + WAIT_LOCKS_COUNT)
#define WAIT_LATCH_OFFSET    (WAIT_IO_OFFSET + WAIT_IO_EVENTS_COUNT)
#define WAIT_NETWORK_OFFSET  (WAIT_LATCH_OFFSET + 1)
#define WAIT_ALLOC_OFFSET    (WAIT_NETWORK_OFFSET + WAIT_NETWORK_EVENTS_COUNT)
#define WAIT_EVENTS_COUNT    (WAIT_ALLOC_OFFSET + 1)

#define WAIT_START(classId, eventId, p1, p2, p3, p4, p5) \
	do { \
		if (WaitsOn) StartWait(classId, eventId, p1, p2, p3, p4, p5);\
	} while(0)

#define WAIT_STOP() \
	do { \
		if (WaitsOn) StopWait(); \
	} while (0);

extern PGDLLIMPORT const char *WAIT_CLASSES[];

typedef struct
{
	uint64 count;		/* count of waits */
	uint64 interval;	/* in microseconds */
} WaitCell;

/* To avoid waste of memory, we keep all waits data in one array,
 * and each class of wait has its offset in that array.
 * Offsets defined in WAIT_OFFSETS const array.
 */
typedef struct
{
	/* indicates that block is buzy (by backend or user query) at current time */
	volatile pg_atomic_flag isBusy;
	/* marks that block is already taken by backend in shared memory */
	volatile pg_atomic_flag isTaken;
	int                     backendPid;
	WaitCell                cells[WAIT_EVENTS_COUNT];
} BackendWaitCells;

void StartWait(int classId, int eventId, int p1, int p2, int p3, int p4, int p5);
void StopWait(void);
void WaitsAllocateShmem(void);
void WaitsFreeBackendCells(PGPROC *proc);
void WaitsInitProcessFields(PGPROC *proc);

Size WaitsShmemSize(void);
const char *WaitsEventName(int classId, int eventId);

extern PGDLLIMPORT bool WaitsOn;
extern PGDLLIMPORT bool WaitsHistoryOn;
extern PGDLLIMPORT int  WaitsFlushPeriod;
extern PGDLLIMPORT int  MaxBackends;
extern PGDLLIMPORT void *WaitShmem;

#endif
