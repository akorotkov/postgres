/*--------------------------------------------------------------------------
 * vodka.h
 *	  Public header file for Generalized Inverted Index access method.
 *
 *	Copyright (c) 2006-2014, PostgreSQL Global Development Group
 *
 *	src/include/access/vodka.h
 *--------------------------------------------------------------------------
 */
#ifndef VODKA_H
#define VODKA_H

#include "access/xlog.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"


/*
 * amproc indexes for inverted indexes.
 */
#define VODKA_CONFIG_PROC			   1
#define VODKA_COMPARE_PROC			   2
#define VODKA_EXTRACTVALUE_PROC		   3
#define VODKA_EXTRACTQUERY_PROC		   4
#define VODKA_CONSISTENT_PROC		   5
#define VODKA_TRICONSISTENT_PROC	   6
#define VODKANProcs					   6

/*
 * Argument structs for vodka_config method
 */
typedef struct VodkaConfigIn
{
	Oid			attType;		/* Data type to be indexed */
} VodkaConfigIn;

typedef struct VodkaConfigOut
{
	Oid			entryOpclass;
	Oid			entryEqualOperator;
} VodkaConfigOut;

typedef struct VodkaKey
{
	Datum		value;
	Pointer		extra;
	Oid			operator;
	bool		isnull;
} VodkaKey;

/*
 * searchMode settings for extractQueryFn.
 */
#define VODKA_SEARCH_MODE_DEFAULT			0
#define VODKA_SEARCH_MODE_INCLUDE_EMPTY		1
#define VODKA_SEARCH_MODE_ALL				2
#define VODKA_SEARCH_MODE_EVERYTHING		3		/* for internal use only */

typedef struct VodkaLastUsedPage
{
	BlockNumber blkno;			/* block number, or InvalidBlockNumber */
	int			freeSpace;		/* page's free space (could be obsolete!) */
} VodkaLastUsedPage;

/*
 * VodkaStatsData represents stats data for planner use
 */
typedef struct VodkaStatsData
{
	BlockNumber nPendingPages;
	BlockNumber nTotalPages;
	BlockNumber nEntryPages;
	BlockNumber nDataPages;
	int64		nEntries;
	int32		vodkaVersion;
	RelFileNode	entryTreeNode;
	bool		hasCleanup;
	VodkaLastUsedPage postingListLUP;
} VodkaStatsData;

/*
 * A ternary value used by tri-consistent functions.
 *
 * For convenience, this is compatible with booleans. A boolean can be
 * safely cast to a VodkaTernaryValue.
 */
typedef char VodkaTernaryValue;

#define VODKA_FALSE             0       /* item is not present / does not match */
#define VODKA_TRUE              1       /* item is present / matches */
#define VODKA_MAYBE             2       /* don't know if item is present / don't know if

                                                       * matches */
#define DatumGetVodkaTernaryValue(X) ((VodkaTernaryValue)(X))
#define VodkaTernaryValueGetDatum(X) ((Datum)(X))
#define PG_RETURN_VODKA_TERNARY_VALUE(x) return VodkaTernaryValueGetDatum(x)

/* GUC parameter */
extern PGDLLIMPORT int VodkaFuzzySearchLimit;

/* vodkautil.c */
extern void vodkaGetStats(Relation index, VodkaStatsData *stats);
extern void vodkaUpdateStats(Relation index, const VodkaStatsData *stats);

/* vodkaxlog.c */
extern void vodka_redo(XLogRecPtr lsn, XLogRecord *record);
extern void vodka_desc(StringInfo buf, uint8 xl_info, char *rec);
extern void vodka_xlog_startup(void);
extern void vodka_xlog_cleanup(void);

#endif   /* VODKA_H */
