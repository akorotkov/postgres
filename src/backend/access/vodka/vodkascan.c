/*-------------------------------------------------------------------------
 *
 * vodkascan.c
 *	  routines to manage scans of inverted index relations
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkascan.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/vodka_private.h"
#include "access/relscan.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/rel.h"


Datum
vodkabeginscan(PG_FUNCTION_ARGS)
{
	Relation	rel = (Relation) PG_GETARG_POINTER(0);
	int			nkeys = PG_GETARG_INT32(1);
	int			norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan;
	VodkaScanOpaque so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (VodkaScanOpaque) palloc(sizeof(VodkaScanOpaqueData));
	so->keys = NULL;
	so->nkeys = 0;
	so->tempCtx = AllocSetContextCreate(CurrentMemoryContext,
										"Vodka scan temporary context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	initVodkaState(&so->vodkastate, scan->indexRelation);

	scan->opaque = so;

	PG_RETURN_POINTER(scan);
}

/*
 * Create a new VodkaScanEntry, unless an equivalent one already exists,
 * in which case just return it
 */
static VodkaScanEntry
vodkaFillScanEntry(VodkaScanOpaque so, OffsetNumber attnum,
				 StrategyNumber strategy, int32 searchMode,
				 Datum queryKey, VodkaNullCategory queryCategory,
				 bool isPartialMatch, Pointer extra_data)
{
	VodkaState   *vodkastate = &so->vodkastate;
	VodkaScanEntry scanEntry;
	uint32		i;

	/*
	 * Look for an existing equivalent entry.
	 *
	 * Entries with non-null extra_data are never considered identical, since
	 * we can't know exactly what the opclass might be doing with that.
	 */
	if (extra_data == NULL)
	{
		for (i = 0; i < so->totalentries; i++)
		{
			VodkaScanEntry prevEntry = so->entries[i];

			if (prevEntry->extra_data == NULL &&
				prevEntry->isPartialMatch == isPartialMatch &&
				prevEntry->strategy == strategy &&
				prevEntry->searchMode == searchMode &&
				prevEntry->attnum == attnum &&
				vodkaCompareEntries(vodkastate, attnum,
								  prevEntry->queryKey,
								  prevEntry->queryCategory,
								  queryKey,
								  queryCategory) == 0)
			{
				/* Successful match */
				return prevEntry;
			}
		}
	}

	/* Nope, create a new entry */
	scanEntry = (VodkaScanEntry) palloc(sizeof(VodkaScanEntryData));
	scanEntry->queryKey = queryKey;
	scanEntry->queryCategory = queryCategory;
	scanEntry->isPartialMatch = isPartialMatch;
	scanEntry->extra_data = extra_data;
	scanEntry->strategy = strategy;
	scanEntry->searchMode = searchMode;
	scanEntry->attnum = attnum;

	scanEntry->buffer = InvalidBuffer;
	ItemPointerSetMin(&scanEntry->curItem);
	scanEntry->matchBitmap = NULL;
	scanEntry->matchIterator = NULL;
	scanEntry->matchResult = NULL;
	scanEntry->list = NULL;
	scanEntry->nlist = 0;
	scanEntry->offset = InvalidOffsetNumber;
	scanEntry->isFinished = false;
	scanEntry->reduceResult = false;

	/* Add it to so's array */
	if (so->totalentries >= so->allocentries)
	{
		so->allocentries *= 2;
		so->entries = (VodkaScanEntry *)
			repalloc(so->entries, so->allocentries * sizeof(VodkaScanEntry));
	}
	so->entries[so->totalentries++] = scanEntry;

	return scanEntry;
}

/*
 * Initialize the next VodkaScanKey using the output from the extractQueryFn
 */
static void
vodkaFillScanKey(VodkaScanOpaque so, OffsetNumber attnum,
			   StrategyNumber strategy, int32 searchMode,
			   Datum query, uint32 nQueryValues,
			   Datum *queryValues, VodkaNullCategory *queryCategories,
			   bool *partial_matches, Pointer *extra_data)
{
	VodkaScanKey	key = &(so->keys[so->nkeys++]);
	VodkaState   *vodkastate = &so->vodkastate;
	uint32		nUserQueryValues = nQueryValues;
	uint32		i;

	/* Non-default search modes add one "hidden" entry to each key */
	if (searchMode != VODKA_SEARCH_MODE_DEFAULT)
		nQueryValues++;
	key->nentries = nQueryValues;
	key->nuserentries = nUserQueryValues;

	key->scanEntry = (VodkaScanEntry *) palloc(sizeof(VodkaScanEntry) * nQueryValues);
	key->entryRes = (bool *) palloc0(sizeof(bool) * nQueryValues);

	key->query = query;
	key->queryValues = queryValues;
	key->queryCategories = queryCategories;
	key->extra_data = extra_data;
	key->strategy = strategy;
	key->searchMode = searchMode;
	key->attnum = attnum;

	ItemPointerSetMin(&key->curItem);
	key->curItemMatches = false;
	key->recheckCurItem = false;
	key->isFinished = false;

	vodkaInitConsistentFunction(vodkastate, key);

	for (i = 0; i < nQueryValues; i++)
	{
		Datum		queryKey;
		VodkaNullCategory queryCategory;
		bool		isPartialMatch;
		Pointer		this_extra;

		if (i < nUserQueryValues)
		{
			/* set up normal entry using extractQueryFn's outputs */
			queryKey = queryValues[i];
			queryCategory = queryCategories[i];
			isPartialMatch =
				(vodkastate->canPartialMatch[attnum - 1] && partial_matches)
				? partial_matches[i] : false;
			this_extra = (extra_data) ? extra_data[i] : NULL;
		}
		else
		{
			/* set up hidden entry */
			queryKey = (Datum) 0;
			switch (searchMode)
			{
				case VODKA_SEARCH_MODE_INCLUDE_EMPTY:
					queryCategory = VODKA_CAT_EMPTY_ITEM;
					break;
				case VODKA_SEARCH_MODE_ALL:
					queryCategory = VODKA_CAT_EMPTY_QUERY;
					break;
				case VODKA_SEARCH_MODE_EVERYTHING:
					queryCategory = VODKA_CAT_EMPTY_QUERY;
					break;
				default:
					elog(ERROR, "unexpected searchMode: %d", searchMode);
					queryCategory = 0;	/* keep compiler quiet */
					break;
			}
			isPartialMatch = false;
			this_extra = NULL;

			/*
			 * We set the strategy to a fixed value so that vodkaFillScanEntry
			 * can combine these entries for different scan keys.  This is
			 * safe because the strategy value in the entry struct is only
			 * used for partial-match cases.  It's OK to overwrite our local
			 * variable here because this is the last loop iteration.
			 */
			strategy = InvalidStrategy;
		}

		key->scanEntry[i] = vodkaFillScanEntry(so, attnum,
											 strategy, searchMode,
											 queryKey, queryCategory,
											 isPartialMatch, this_extra);
	}
}

static void
freeScanKeys(VodkaScanOpaque so)
{
	uint32		i;

	if (so->keys == NULL)
		return;

	for (i = 0; i < so->nkeys; i++)
	{
		VodkaScanKey	key = so->keys + i;

		pfree(key->scanEntry);
		pfree(key->entryRes);
	}

	pfree(so->keys);
	so->keys = NULL;
	so->nkeys = 0;

	for (i = 0; i < so->totalentries; i++)
	{
		VodkaScanEntry entry = so->entries[i];

		if (entry->buffer != InvalidBuffer)
			ReleaseBuffer(entry->buffer);
		if (entry->list)
			pfree(entry->list);
		if (entry->matchIterator)
			tbm_end_iterate(entry->matchIterator);
		if (entry->matchBitmap)
			tbm_free(entry->matchBitmap);
		pfree(entry);
	}

	pfree(so->entries);
	so->entries = NULL;
	so->totalentries = 0;
}

void
vodkaNewScanKey(IndexScanDesc scan)
{
	ScanKey		scankey = scan->keyData;
	VodkaScanOpaque so = (VodkaScanOpaque) scan->opaque;
	int			i;
	bool		hasNullQuery = false;

	/* if no scan keys provided, allocate extra EVERYTHING VodkaScanKey */
	so->keys = (VodkaScanKey)
		palloc(Max(scan->numberOfKeys, 1) * sizeof(VodkaScanKeyData));
	so->nkeys = 0;

	/* initialize expansible array of VodkaScanEntry pointers */
	so->totalentries = 0;
	so->allocentries = 32;
	so->entries = (VodkaScanEntry *)
		palloc0(so->allocentries * sizeof(VodkaScanEntry));

	so->isVoidRes = false;

	for (i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		skey = &scankey[i];
		Datum	   *queryValues;
		int32		nQueryValues = 0;
		bool	   *partial_matches = NULL;
		Pointer    *extra_data = NULL;
		bool	   *nullFlags = NULL;
		int32		searchMode = VODKA_SEARCH_MODE_DEFAULT;

		/*
		 * We assume that VODKA-indexable operators are strict, so a null query
		 * argument means an unsatisfiable query.
		 */
		if (skey->sk_flags & SK_ISNULL)
		{
			so->isVoidRes = true;
			break;
		}

		/* OK to call the extractQueryFn */
		queryValues = (Datum *)
			DatumGetPointer(FunctionCall7Coll(&so->vodkastate.extractQueryFn[skey->sk_attno - 1],
						   so->vodkastate.supportCollation[skey->sk_attno - 1],
											  skey->sk_argument,
											  PointerGetDatum(&nQueryValues),
										   UInt16GetDatum(skey->sk_strategy),
										   PointerGetDatum(&partial_matches),
											  PointerGetDatum(&extra_data),
											  PointerGetDatum(&nullFlags),
											  PointerGetDatum(&searchMode)));

		/*
		 * If bogus searchMode is returned, treat as VODKA_SEARCH_MODE_ALL; note
		 * in particular we don't allow extractQueryFn to select
		 * VODKA_SEARCH_MODE_EVERYTHING.
		 */
		if (searchMode < VODKA_SEARCH_MODE_DEFAULT ||
			searchMode > VODKA_SEARCH_MODE_ALL)
			searchMode = VODKA_SEARCH_MODE_ALL;

		/* Non-default modes require the index to have placeholders */
		if (searchMode != VODKA_SEARCH_MODE_DEFAULT)
			hasNullQuery = true;

		/*
		 * In default mode, no keys means an unsatisfiable query.
		 */
		if (queryValues == NULL || nQueryValues <= 0)
		{
			if (searchMode == VODKA_SEARCH_MODE_DEFAULT)
			{
				so->isVoidRes = true;
				break;
			}
			nQueryValues = 0;	/* ensure sane value */
		}

		/*
		 * If the extractQueryFn didn't create a nullFlags array, create one,
		 * assuming that everything's non-null.  Otherwise, run through the
		 * array and make sure each value is exactly 0 or 1; this ensures
		 * binary compatibility with the VodkaNullCategory representation. While
		 * at it, detect whether any null keys are present.
		 */
		if (nullFlags == NULL)
			nullFlags = (bool *) palloc0(nQueryValues * sizeof(bool));
		else
		{
			int32		j;

			for (j = 0; j < nQueryValues; j++)
			{
				if (nullFlags[j])
				{
					nullFlags[j] = true;		/* not any other nonzero value */
					hasNullQuery = true;
				}
			}
		}
		/* now we can use the nullFlags as category codes */

		vodkaFillScanKey(so, skey->sk_attno,
					   skey->sk_strategy, searchMode,
					   skey->sk_argument, nQueryValues,
					   queryValues, (VodkaNullCategory *) nullFlags,
					   partial_matches, extra_data);
	}

	/*
	 * If there are no regular scan keys, generate an EVERYTHING scankey to
	 * drive a full-index scan.
	 */
	if (so->nkeys == 0 && !so->isVoidRes)
	{
		hasNullQuery = true;
		vodkaFillScanKey(so, FirstOffsetNumber,
					   InvalidStrategy, VODKA_SEARCH_MODE_EVERYTHING,
					   (Datum) 0, 0,
					   NULL, NULL, NULL, NULL);
	}

	/*
	 * If the index is version 0, it may be missing null and placeholder
	 * entries, which would render searches for nulls and full-index scans
	 * unreliable.	Throw an error if so.
	 */
	if (hasNullQuery && !so->isVoidRes)
	{
		VodkaStatsData vodkaStats;

		vodkaGetStats(scan->indexRelation, &vodkaStats);
		if (vodkaStats.vodkaVersion < 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("old VODKA indexes do not support whole-index scans nor searches for nulls"),
					 errhint("To fix this, do REINDEX INDEX \"%s\".",
							 RelationGetRelationName(scan->indexRelation))));
	}

	pgstat_count_index_scan(scan->indexRelation);
}

Datum
vodkarescan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey		scankey = (ScanKey) PG_GETARG_POINTER(1);

	/* remaining arguments are ignored */
	VodkaScanOpaque so = (VodkaScanOpaque) scan->opaque;

	freeScanKeys(so);

	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}

	PG_RETURN_VOID();
}


Datum
vodkaendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	VodkaScanOpaque so = (VodkaScanOpaque) scan->opaque;

	freeScanKeys(so);

	MemoryContextDelete(so->tempCtx);

	pfree(so);

	PG_RETURN_VOID();
}

Datum
vodkamarkpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "VODKA does not support mark/restore");
	PG_RETURN_VOID();
}

Datum
vodkarestrpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "VODKA does not support mark/restore");
	PG_RETURN_VOID();
}
