/*-------------------------------------------------------------------------
 *
 * vodkainsert.c
 *	  insert routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkainsert.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "access/vodka_private.h"
#include "access/heapam_xlog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/pg_operator.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"


typedef struct
{
	VodkaState	vodkastate;
	double		indtuples;
	VodkaStatsData buildStats;
	MemoryContext tmpCtx;
	MemoryContext funcCtx;
	BuildAccumulator accum;
} VodkaBuildState;


#ifdef NOT_USED
/*
 * Adds array of item pointers to tuple's posting list, or
 * creates posting tree and tuple pointing to tree in case
 * of not enough space.  Max size of tuple is defined in
 * VodkaFormTuple().	Returns a new, modified index tuple.
 * items[] must be in sorted order with no duplicates.
 */
static IndexTuple
addItemPointersToLeafTuple(VodkaState *vodkastate,
						   IndexTuple old,
						   ItemPointerData *items, uint32 nitem,
						   VodkaStatsData *buildStats)
{
	OffsetNumber attnum;
	Datum		key;
	VodkaNullCategory category;
	IndexTuple	res;
	ItemPointerData *newItems,
			   *oldItems;
	int			oldNPosting,
				newNPosting;
	VodkaPostingList *compressedList;

	Assert(!VodkaIsPostingTree(old));

	attnum = vodkatuple_get_attrnum(vodkastate, old);
	key = vodkatuple_get_key(vodkastate, old, &category);

	/* merge the old and new posting lists */
	oldItems = vodkaReadTuple(vodkastate, attnum, old, &oldNPosting);

	newNPosting = oldNPosting + nitem;
	newItems = (ItemPointerData *) palloc(sizeof(ItemPointerData) * newNPosting);

	newNPosting = vodkaMergeItemPointers(newItems,
									   items, nitem,
									   oldItems, oldNPosting);

	/* Compress the posting list, and try to a build tuple with room for it */
	res = NULL;
	compressedList = vodkaCompressPostingList(newItems, newNPosting, VodkaMaxItemSize,
											NULL);
	pfree(newItems);
	if (compressedList)
	{
		res = VodkaFormTuple(vodkastate, attnum, key, category,
						   (char *) compressedList,
						   SizeOfVodkaPostingList(compressedList),
						   newNPosting,
						   false);
		pfree(compressedList);
	}
	if (!res)
	{
		/* posting list would be too big, convert to posting tree */
		BlockNumber postingRoot;

		/*
		 * Initialize posting tree with the old tuple's posting list.  It's
		 * surely small enough to fit on one posting-tree page, and should
		 * already be in order with no duplicates.
		 */
		postingRoot = vodkaCreatePostingTree(vodkastate->index,
										oldItems,
										oldNPosting,
										buildStats);

		/* Now insert the TIDs-to-be-added into the posting tree */
		vodkaInsertItemPointers(vodkastate->index, postingRoot,
							  items, nitem,
							  buildStats);

		/* And build a new posting-tree-only result tuple */
		res = VodkaFormTuple(vodkastate, attnum, key, category, NULL, 0, 0, true);
		VodkaSetPostingTree(res, postingRoot);
	}
	pfree(oldItems);

	return res;
}
#endif

#ifdef NOT_USED
/*
 * Build a fresh leaf tuple, either posting-list or posting-tree format
 * depending on whether the given items list will fit.
 * items[] must be in sorted order with no duplicates.
 *
 * This is basically the same logic as in addItemPointersToLeafTuple,
 * but working from slightly different input.
 */
static IndexTuple
buildFreshLeafTuple(VodkaState *vodkastate,
					OffsetNumber attnum, Datum key, VodkaNullCategory category,
					ItemPointerData *items, uint32 nitem,
					VodkaStatsData *buildStats)
{
	IndexTuple	res = NULL;
	VodkaPostingList *compressedList;

	/* try to build a posting list tuple with all the items */
	compressedList = vodkaCompressPostingList(items, nitem, VodkaMaxItemSize, NULL);
	if (compressedList)
	{
		res = VodkaFormTuple(vodkastate, attnum, key, category,
						   (char *) compressedList,
						   SizeOfVodkaPostingList(compressedList),
						   nitem, false);
		pfree(compressedList);
	}
	if (!res)
	{
		/* posting list would be too big, build posting tree */
		BlockNumber postingRoot;

		/*
		 * Build posting-tree-only result tuple.  We do this first so as to
		 * fail quickly if the key is too big.
		 */
		res = VodkaFormTuple(vodkastate, attnum, key, category, NULL, 0, 0, true);

		/*
		 * Initialize a new posting tree with the TIDs.
		 */
		postingRoot = vodkaCreatePostingTree(vodkastate->index, items, nitem,
										buildStats);

		/* And save the root link in the result tuple */
		VodkaSetPostingTree(res, postingRoot);
	}

	return res;
}
#endif

/*
 * Insert one or more heap TIDs associated with the given key value.
 * This will either add a single key entry, or enlarge a pre-existing entry.
 *
 * During an index build, buildStats is non-null and the counters
 * it contains should be incremented as needed.
 */
void
vodkaEntryInsert(VodkaState *vodkastate,
			   OffsetNumber attnum, Datum key, VodkaNullCategory category,
			   ItemPointerData *items, uint32 nitem,
			   VodkaStatsData *buildStats)
{
#ifdef NOT_USED
	VodkaBtreeData btree;
	VodkaBtreeEntryInsertData insertdata;
	VodkaBtreeStack *stack;
	IndexTuple	itup;
	Page		page;
#endif
	bool		isnull, found;
	ItemPointerData	iptr;
	IndexScanDesc	equalScan;
	BlockNumber postingRoot;

	/*insertdata.isDelete = FALSE;*/

	/* During index build, count the to-be-inserted entry */
	if (buildStats)
		buildStats->nEntries++;

#ifdef NOT_USED
	vodkaPrepareEntryScan(&btree, attnum, key, category, vodkastate);

	stack = vodkaFindLeafPage(&btree, false);
	page = BufferGetPage(stack->buffer);

	if (btree.findItem(&btree, stack))
	{
		/* found pre-existing entry */
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, stack->off));

		if (VodkaIsPostingTree(itup))
		{
			/* add entries to existing posting tree */
			BlockNumber rootPostingTree = VodkaGetPostingTree(itup);

			/* release all stack */
			LockBuffer(stack->buffer, VODKA_UNLOCK);
			freeVodkaBtreeStack(stack);

			/* insert into posting tree */
			vodkaInsertItemPointers(vodkastate->index, rootPostingTree,
								  items, nitem,
								  buildStats);
			return;
		}

		/* modify an existing leaf entry */
		itup = addItemPointersToLeafTuple(vodkastate, itup,
										  items, nitem, buildStats);

		insertdata.isDelete = TRUE;
	}
	else
	{
		/* no match, so construct a new leaf entry */
		itup = buildFreshLeafTuple(vodkastate, attnum, key, category,
								   items, nitem, buildStats);
	}

	/* Insert the new or modified leaf tuple */
	insertdata.entry = itup;
	vodkaInsertValue(&btree, stack, &insertdata, buildStats);
#endif

	vodkastate->entryEqualScan = prepareEntryIndexScan(vodkastate,
			TextEqualOperator, key);

	equalScan = vodkastate->entryEqualScan;

	found =	DatumGetBool(OidFunctionCall2(vodkastate->entryTree.rd_am->amgettuple,
						 PointerGetDatum(equalScan),
						 Int32GetDatum(ForwardScanDirection)));

	if (found)
		postingRoot = ItemPointerGetBlockNumber(&equalScan->xs_ctup.t_self);

	OidFunctionCall1(vodkastate->entryTree.rd_am->amendscan,
						 PointerGetDatum(vodkastate->entryEqualScan));

	if (found)
	{
		vodkaInsertItemPointers(vodkastate->index, postingRoot,
							  items, nitem,
							  buildStats);
	}
	else
	{
		postingRoot = vodkaCreatePostingTree(vodkastate->index, items, nitem,
										buildStats);

		isnull = (category != VODKA_CAT_NORM_KEY);

		ItemPointerSetBlockNumber(&iptr, postingRoot);
		iptr.ip_posid = 1;

		OidFunctionCall4(vodkastate->entryTree.rd_am->aminsert,
						 PointerGetDatum(&vodkastate->entryTree),
						 PointerGetDatum(&key),
						 PointerGetDatum(&isnull),
						 PointerGetDatum(&iptr));
	}

	/*pfree(itup);*/
}

/*
 * Extract index entries for a single indexable item, and add them to the
 * BuildAccumulator's state.
 *
 * This function is used only during initial index creation.
 */
static void
vodkaHeapTupleBulkInsert(VodkaBuildState *buildstate, OffsetNumber attnum,
					   Datum value, bool isNull,
					   ItemPointer heapptr)
{
	Datum	   *entries;
	VodkaNullCategory *categories;
	int32		nentries;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(buildstate->funcCtx);
	entries = vodkaExtractEntries(buildstate->accum.vodkastate, attnum,
								value, isNull,
								&nentries, &categories);
	MemoryContextSwitchTo(oldCtx);

	vodkaInsertBAEntries(&buildstate->accum, heapptr, attnum,
					   entries, categories, nentries);

	buildstate->indtuples += nentries;

	MemoryContextReset(buildstate->funcCtx);
}

static void
vodkaBuildCallback(Relation index, HeapTuple htup, Datum *values,
				 bool *isnull, bool tupleIsAlive, void *state)
{
	VodkaBuildState *buildstate = (VodkaBuildState *) state;
	MemoryContext oldCtx;
	int			i;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	for (i = 0; i < buildstate->vodkastate.origTupdesc->natts; i++)
		vodkaHeapTupleBulkInsert(buildstate, (OffsetNumber) (i + 1),
							   values[i], isnull[i],
							   &htup->t_self);

	/* If we've maxed out our available memory, dump everything to the index */
	if (buildstate->accum.allocatedMemory >= maintenance_work_mem * 1024L)
	{
		ItemPointerData *list;
		Datum		key;
		VodkaNullCategory category;
		uint32		nlist;
		OffsetNumber attnum;

		vodkaBeginBAScan(&buildstate->accum);
		while ((list = vodkaGetBAEntry(&buildstate->accum,
								  &attnum, &key, &category, &nlist)) != NULL)
		{
			/* there could be many entries, so be willing to abort here */
			CHECK_FOR_INTERRUPTS();
			vodkaEntryInsert(&buildstate->vodkastate, attnum, key, category,
						   list, nlist, &buildstate->buildStats);
		}

		MemoryContextReset(buildstate->tmpCtx);
		vodkaInitBA(&buildstate->accum);
	}

	MemoryContextSwitchTo(oldCtx);
}

Datum
vodkabuild(PG_FUNCTION_ARGS)
{
	Relation	heap = (Relation) PG_GETARG_POINTER(0);
	Relation	index = (Relation) PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
	IndexBuildResult *result;
	double		reltuples;
	VodkaBuildState buildstate;
	Buffer		RootBuffer,
				MetaBuffer;
	ItemPointerData *list;
	Datum		key;
	VodkaNullCategory category;
	uint32		nlist;
	MemoryContext oldCtx;
	OffsetNumber attnum;
	IndexBuildResult *stats;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* initialize the meta page */
	MetaBuffer = VodkaNewBuffer(index);

	/* initialize the root page */
	RootBuffer = VodkaNewBuffer(index);

	START_CRIT_SECTION();
	VodkaInitMetabuffer(index, MetaBuffer);
	MarkBufferDirty(MetaBuffer);
	VodkaInitBuffer(RootBuffer, VODKA_LEAF);
	MarkBufferDirty(RootBuffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata;
		Page		page;

		rdata.buffer = InvalidBuffer;
		rdata.data = (char *) &(index->rd_node);
		rdata.len = sizeof(RelFileNode);
		rdata.next = NULL;

		recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_CREATE_INDEX, &rdata);

		page = BufferGetPage(RootBuffer);
		PageSetLSN(page, recptr);

		page = BufferGetPage(MetaBuffer);
		PageSetLSN(page, recptr);
	}

	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(RootBuffer);
	END_CRIT_SECTION();

	initVodkaState(&buildstate.vodkastate, index);

	RelationOpenSmgr(&buildstate.vodkastate.entryTree);
	RelationCreateStorage(buildstate.vodkastate.entryTree.rd_node,
			buildstate.vodkastate.entryTree.rd_rel->relpersistence);

	stats = (IndexBuildResult *)
		DatumGetPointer(OidFunctionCall3(buildstate.vodkastate.entryTree.rd_am->ambuild,
						 PointerGetDatum(NULL),
						 PointerGetDatum(&buildstate.vodkastate.entryTree),
						 PointerGetDatum(NULL)));

	buildstate.indtuples = 0;
	memset(&buildstate.buildStats, 0, sizeof(VodkaStatsData));

	/* count the root as first entry page */
	buildstate.buildStats.nEntryPages++;

	/*
	 * create a temporary memory context that is reset once for each tuple
	 * inserted into the index
	 */
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Vodka build temporary context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.funcCtx = AllocSetContextCreate(buildstate.tmpCtx,
					 "Vodka build temporary context for user-defined function",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.accum.vodkastate = &buildstate.vodkastate;
	vodkaInitBA(&buildstate.accum);

	/*
	 * Do the heap scan.  We disallow sync scan here because dataPlaceToPage
	 * prefers to receive tuples in TID order.
	 */
	reltuples = IndexBuildHeapScan(heap, index, indexInfo, false,
								   vodkaBuildCallback, (void *) &buildstate);

	/* dump remaining entries to the index */
	oldCtx = MemoryContextSwitchTo(buildstate.tmpCtx);
	vodkaBeginBAScan(&buildstate.accum);
	while ((list = vodkaGetBAEntry(&buildstate.accum,
								 &attnum, &key, &category, &nlist)) != NULL)
	{
		/* there could be many entries, so be willing to abort here */
		CHECK_FOR_INTERRUPTS();
		vodkaEntryInsert(&buildstate.vodkastate, attnum, key, category,
					   list, nlist, &buildstate.buildStats);
	}
	MemoryContextSwitchTo(oldCtx);

	MemoryContextDelete(buildstate.tmpCtx);

	/*
	 * Update metapage stats
	 */
	buildstate.buildStats.nTotalPages = RelationGetNumberOfBlocks(index);
	vodkaUpdateStats(index, &buildstate.buildStats);

	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	freeVodkaState(&buildstate.vodkastate);

	PG_RETURN_POINTER(result);
}

/*
 *	vodkabuildempty() -- build an empty vodka index in the initialization fork
 */
Datum
vodkabuildempty(PG_FUNCTION_ARGS)
{
	Relation	index = (Relation) PG_GETARG_POINTER(0);
	Buffer		RootBuffer,
				MetaBuffer;
	VodkaState	vodkastate;
	IndexBuildResult *stats;

	/* An empty VODKA index has two pages. */
	MetaBuffer =
		ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(MetaBuffer, BUFFER_LOCK_EXCLUSIVE);
	RootBuffer =
		ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(RootBuffer, BUFFER_LOCK_EXCLUSIVE);

	/* Initialize and xlog metabuffer and root buffer. */
	START_CRIT_SECTION();
	VodkaInitMetabuffer(index, MetaBuffer);
	MarkBufferDirty(MetaBuffer);
	log_newpage_buffer(MetaBuffer, false);
	VodkaInitBuffer(RootBuffer, VODKA_LEAF);
	MarkBufferDirty(RootBuffer);
	log_newpage_buffer(RootBuffer, false);
	END_CRIT_SECTION();

	initVodkaState(&vodkastate, index);

	/* Unlock and release the buffers. */
	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(RootBuffer);

	RelationOpenSmgr(&vodkastate.entryTree);
	RelationCreateStorage(vodkastate.entryTree.rd_node,
			vodkastate.entryTree.rd_rel->relpersistence);

	stats = (IndexBuildResult *)
		DatumGetPointer(OidFunctionCall3(vodkastate.entryTree.rd_am->ambuild,
						 PointerGetDatum(NULL),
						 PointerGetDatum(&vodkastate.entryTree),
						 PointerGetDatum(NULL)));

	freeVodkaState(&vodkastate);

	PG_RETURN_VOID();
}

/*
 * Insert index entries for a single indexable item during "normal"
 * (non-fast-update) insertion
 */
static void
vodkaHeapTupleInsert(VodkaState *vodkastate, OffsetNumber attnum,
				   Datum value, bool isNull,
				   ItemPointer item)
{
	Datum	   *entries;
	VodkaNullCategory *categories;
	int32		i,
				nentries;

	entries = vodkaExtractEntries(vodkastate, attnum, value, isNull,
								&nentries, &categories);

	for (i = 0; i < nentries; i++)
		vodkaEntryInsert(vodkastate, attnum, entries[i], categories[i],
					   item, 1, NULL);
}

Datum
vodkainsert(PG_FUNCTION_ARGS)
{
	Relation	index = (Relation) PG_GETARG_POINTER(0);
	Datum	   *values = (Datum *) PG_GETARG_POINTER(1);
	bool	   *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer ht_ctid = (ItemPointer) PG_GETARG_POINTER(3);

#ifdef NOT_USED
	Relation	heapRel = (Relation) PG_GETARG_POINTER(4);
	IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);
#endif
	VodkaState	vodkastate;
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	int			i;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Vodka insert temporary context",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	initVodkaState(&vodkastate, index);

	/*if (VodkaGetUseFastUpdate(index))
	{
		VodkaTupleCollector collector;

		memset(&collector, 0, sizeof(VodkaTupleCollector));

		for (i = 0; i < vodkastate.origTupdesc->natts; i++)
			vodkaHeapTupleFastCollect(&vodkastate, &collector,
									(OffsetNumber) (i + 1),
									values[i], isnull[i],
									ht_ctid);

		vodkaHeapTupleFastInsert(&vodkastate, &collector);
	}
	else
	{*/
		for (i = 0; i < vodkastate.origTupdesc->natts; i++)
			vodkaHeapTupleInsert(&vodkastate, (OffsetNumber) (i + 1),
							   values[i], isnull[i],
							   ht_ctid);
	/*}*/

	freeVodkaState(&vodkastate);
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	PG_RETURN_BOOL(false);
}
