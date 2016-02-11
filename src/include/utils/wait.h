#ifndef _WAIT_H_
#define _WAIT_H_

#include "storage/lwlock.h"
#include "storage/proc.h"

/*
 * Waits logging
 *
 * There are two main functions: StartWait and StopWait.
 * StartWait is called at the beginning, StopWait at the end of wait.
 * Every wait has it's own parameters. Parameters count must be equal with
 * WAIT_PARAMS_COUNT. Every backend contains sum of intervals and count for
 * each wait. GUC `waits_flush_period` regulate how often data from backend
 * will be flushed to shared memory and will be visible in views.
 * In shared memory we allocate space that is enough to hold data for all
 * backends. When process is starting it reservers block in shared memory,
 * when dies it marks that the block is free.
 *
 * Monitored waits by now:
 *
 * 1) CPU events. For now it is on memory chunks allocation
 * 2) Heavy-weight locks (lock.c)
 * 3) LW-Locks (lwlock.c)
 * 4) IO read-write (md.c)
 * 5) Network (be-secure.c)
 * 6) Latches (pg_latch.c)
 */

enum WaitClass
{
	WAIT_CPU = 0,
	WAIT_LWLOCK,
	WAIT_LOCK,
	WAIT_IO,
	WAIT_LATCH,
	WAIT_NETWORK,
	/* Last item as count */
	WAIT_CLASSES_COUNT
} WaitClass;

enum WaitCPUEvent
{
	WAIT_CPU_BUSY = 0,
	WAIT_MALLOC = 1,
	/* Last item as count */
	WAIT_CPU_EVENTS_COUNT
} WaitCPUEvent;

enum WaitIOEvent
{
	WAIT_SMGR_READ = 0,
	WAIT_SMGR_WRITE,
	WAIT_SMGR_FSYNC,
	WAIT_SMGR_EXTEND,
	WAIT_XLOG_READ,
	WAIT_XLOG_WRITE,
	WAIT_XLOG_FSYNC,
	WAIT_SLRU_READ,
	WAIT_SLRU_WRITE,
	WAIT_SLRU_FSYNC,
	/* Last item as count */
	WAIT_IO_EVENTS_COUNT
} WaitIOEvent;

enum WaitNetworkEvent
{
	WAIT_NETWORK_READ = 0,
	WAIT_NETWORK_WRITE,
	WAIT_SYSLOG,
	/* last item as count */
	WAIT_NETWORK_EVENTS_COUNT
} WaitNetworkEvent;

#define WAIT_LWLOCKS_COUNT	(NUM_INDIVIDUAL_LWLOCKS + LWTRANCHE_FIRST_USER_DEFINED - 1)
#define WAIT_LOCKS_COUNT	(LOCKTAG_LAST_TYPE + 1)
#define WAIT_PARAMS_COUNT	5

typedef void (*wait_event_start_hook_type) (uint32 classid, uint32 eventid, uint32 p1, uint32 p2, uint32 p3, uint32 p4, uint32 p5);
typedef void (*wait_event_stop_hook_type) (void);

extern wait_event_start_hook_type wait_event_start_hook;
extern wait_event_stop_hook_type wait_event_stop_hook;

#define WAIT_START(classId, eventId, p1, p2, p3, p4, p5) \
	do { \
		if (wait_event_start_hook) \
			wait_event_start_hook(classId, eventId, p1, p2, p3, p4, p5);\
	} while(0)

#define WAIT_STOP() \
	do { \
		if (wait_event_stop_hook) \
			wait_event_stop_hook(); \
	} while (0);

#endif
