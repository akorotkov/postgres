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
#include "utils/relcache.h"


/*
 * amproc indexes for inverted indexes.
 */
#define VODKA_COMPARE_PROC			   1
#define VODKA_EXTRACTVALUE_PROC		   2
#define VODKA_EXTRACTQUERY_PROC		   3
#define VODKA_CONSISTENT_PROC		   4
#define VODKA_COMPARE_PARTIAL_PROC	   5
#define VODKANProcs					   5

/*
 * searchMode settings for extractQueryFn.
 */
#define VODKA_SEARCH_MODE_DEFAULT			0
#define VODKA_SEARCH_MODE_INCLUDE_EMPTY		1
#define VODKA_SEARCH_MODE_ALL				2
#define VODKA_SEARCH_MODE_EVERYTHING		3		/* for internal use only */

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
} VodkaStatsData;

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
