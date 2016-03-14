/*
 * pg_stat_wait.h
 *		Headers for pg_stat_wait extension.
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_wait/pg_stat_waits.h
 */
#ifndef __PG_STAT_WAIT_H__
#define __PG_STAT_WAIT_H__

#include <postgres.h>

#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "utils/timestamp.h"
#include "utils/wait.h"

#define	PG_STAT_WAIT_MAGIC		0xca94b107
#define COLLECTOR_QUEUE_SIZE	16384
#define HISTORY_TIME_MULTIPLIER	10
#define HISTORY_COLUMNS_COUNT	(5 + WAIT_PARAMS_COUNT)

typedef struct
{
	uint32		classeventid;
	uint32		params[WAIT_PARAMS_COUNT];
	instr_time	start_time;
} CurrentWaitEvent;

typedef struct
{
	uint32				curidx;
	CurrentWaitEvent	data[2];
} CurrentWaitEventWrap;

typedef struct
{
	uint64			count;
	uint64			interval;
} ProfileItem;

typedef struct
{
	bool			reset;
	int				procIdx;
	uint32			procPid;
	int				classIdx;
	int				eventIdx;
	ProfileItem	   *item;
} WaitProfileContext;

typedef struct
{
	int			class_idx;
	int			event_idx;
} WaitEventContext;

typedef struct
{
	uint32		classid;
	uint32		eventid;
	uint32		params[WAIT_PARAMS_COUNT];
	int			backendPid;
	uint64		waitTime;

	TimestampTz	ts;
} HistoryItem;

typedef struct
{
	HistoryItem	   *state;
	TimestampTz		ts;
} WaitCurrentContext;

typedef struct
{
	bool			wraparound;
	int				index;
	int				count;
	HistoryItem	   *items;
} History;

typedef enum
{
	NO_REQUEST,
	HISTORY_REQUEST
} SHMRequest;

typedef struct
{
	Latch		   *latch;
	SHMRequest		request;
} CollectorShmqHeader;

#define WAIT_EVENTS_COUNT (WAIT_CPU_EVENTS_COUNT + 1 + WAIT_LWLOCKS_COUNT + \
						   WAIT_LOCKS_COUNT + WAIT_IO_EVENTS_COUNT + \
						   WAIT_NETWORK_EVENTS_COUNT)

#define WAIT_TRACE_FN_LEN	(4096)

typedef struct
{
	bool			traceOn;
	char			filename[WAIT_TRACE_FN_LEN + 1];
	FILE		   *fd;
} TraceInfo;

/* pg_stat_wait.c */
extern void check_shmem(void);
extern CollectorShmqHeader *collector_hdr;
extern CurrentWaitEventWrap *cur_wait_events;
extern shm_mq			   *collector_mq;
extern int					historySize;
extern int					historyPeriod;
extern bool					historySkipLatch;


extern void RegisterWaitsCollector(void);
extern void AllocHistory(History *, int);
extern void ReadCurrentWait(PGPROC *proc, HistoryItem *item);

/* descr.c */
extern const int numberOfEvents[];
const char *getWaitClassName(uint32 classid);
const char *getWaitEventName(uint32 classid, uint32 eventid);

#endif
