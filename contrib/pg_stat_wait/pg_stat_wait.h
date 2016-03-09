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
	uint32		backendPid;
	bool		reset;
	int			backendIdx;
	int			classIdx;
	int			eventIdx;
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


#define WAIT_TRACE_FN_LEN	(4096 + 1)

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

#endif
