/*-------------------------------------------------------------------------
 *
 * vodkavacuum.c
 *	  delete & vacuum routines for the postgres VODKA
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkavacuum.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/vodka_private.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

struct VodkaVacuumState
{
	Relation	index;
	IndexBulkDeleteResult *result;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	VodkaState	vodkastate;
	BufferAccessStrategy strategy;
	MemoryContext tmpCxt;
};

/*
 * Vacuums an uncompressed posting list. The size of the must can be specified
 * in number of items (nitems).
 *
 * If none of the items need to be removed, returns NULL. Otherwise returns
 * a new palloc'd array with the remaining items. The number of remaining
 * items is returned in *nremaining.
 */
ItemPointer
vodkaVacuumItemPointers(VodkaVacuumState *gvs, ItemPointerData *items,
					  int nitem, int *nremaining)
{
	int			i,
				remaining = 0;
	ItemPointer	tmpitems = NULL;

	/*
	 * Iterate over TIDs array
	 */
	for (i = 0; i < nitem; i++)
	{
		if (gvs->callback(items + i, gvs->callback_state))
		{
			gvs->result->tuples_removed += 1;
			if (!tmpitems)
			{
				/*
				 * First TID to be deleted: allocate memory to hold the
				 * remaining items.
				 */
				tmpitems = palloc(sizeof(ItemPointerData) * nitem);
				memcpy(tmpitems, items, sizeof(ItemPointerData) * i);
			}
		}
		else
		{
			gvs->result->num_index_tuples += 1;
			if (tmpitems)
				tmpitems[remaining] = items[i];
			remaining++;
		}
	}

	*nremaining = remaining;
	return tmpitems;
}

/*
 * Create a WAL record for vacuuming entry tree leaf page.
 */
static void
xlogVacuumPage(Relation index, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	XLogRecPtr	recptr;
	XLogRecData rdata[3];
	vodkaxlogVacuumPage xlrec;
	uint16		lower;
	uint16		upper;

	/* This is only used for entry tree leaf pages. */
	Assert(!VodkaPageIsData(page));
	Assert(VodkaPageIsLeaf(page));

	if (!RelationNeedsWAL(index))
		return;

	xlrec.node = index->rd_node;
	xlrec.blkno = BufferGetBlockNumber(buffer);

	/* Assume we can omit data between pd_lower and pd_upper */
	lower = ((PageHeader) page)->pd_lower;
	upper = ((PageHeader) page)->pd_upper;

	Assert(lower < BLCKSZ);
	Assert(upper < BLCKSZ);

	if (lower >= SizeOfPageHeaderData &&
		upper > lower &&
		upper <= BLCKSZ)
	{
		xlrec.hole_offset = lower;
		xlrec.hole_length = upper - lower;
	}
	else
	{
		/* No "hole" to compress out */
		xlrec.hole_offset = 0;
		xlrec.hole_length = 0;
	}

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = sizeof(vodkaxlogVacuumPage);
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &rdata[1];

	if (xlrec.hole_length == 0)
	{
		rdata[1].data = (char *) page;
		rdata[1].len = BLCKSZ;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;
	}
	else
	{
		/* must skip the hole */
		rdata[1].data = (char *) page;
		rdata[1].len = xlrec.hole_offset;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = &rdata[2];

		rdata[2].data = (char *) page + (xlrec.hole_offset + xlrec.hole_length);
		rdata[2].len = BLCKSZ - (xlrec.hole_offset + xlrec.hole_length);
		rdata[2].buffer = InvalidBuffer;
		rdata[2].next = NULL;
	}

	recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_VACUUM_PAGE, rdata);
	PageSetLSN(page, recptr);
}

static bool
vodkaVacuumPostingTreeLeaves(VodkaVacuumState *gvs, BlockNumber blkno, bool isRoot, Buffer *rootBuffer)
{
	Buffer		buffer;
	Page		page;
	bool		hasVoidPage = FALSE;
	MemoryContext oldCxt;

	buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, gvs->strategy);
	page = BufferGetPage(buffer);

	/*
	 * We should be sure that we don't concurrent with inserts, insert process
	 * never release root page until end (but it can unlock it and lock
	 * again). New scan can't start but previously started ones work
	 * concurrently.
	 */
	if (isRoot)
		LockBufferForCleanup(buffer);
	else
		LockBuffer(buffer, VODKA_EXCLUSIVE);

	Assert(VodkaPageIsData(page));

	if (VodkaPageIsLeaf(page))
	{
		oldCxt = MemoryContextSwitchTo(gvs->tmpCxt);
		vodkaVacuumPostingTreeLeaf(gvs->index, buffer, gvs);
		MemoryContextSwitchTo(oldCxt);
		MemoryContextReset(gvs->tmpCxt);

		/* if root is a leaf page, we don't desire further processing */
		if (!isRoot && !hasVoidPage && VodkaDataLeafPageIsEmpty(page))
			hasVoidPage = TRUE;
	}
	else
	{
		OffsetNumber i;
		bool		isChildHasVoid = FALSE;

		for (i = FirstOffsetNumber; i <= VodkaPageGetOpaque(page)->maxoff; i++)
		{
			PostingItem *pitem = VodkaDataPageGetPostingItem(page, i);

			if (vodkaVacuumPostingTreeLeaves(gvs, PostingItemGetBlockNumber(pitem), FALSE, NULL))
				isChildHasVoid = TRUE;
		}

		if (isChildHasVoid)
			hasVoidPage = TRUE;
	}

	/*
	 * if we have root and there are empty pages in tree, then we don't release
	 * lock to go further processing and guarantee that tree is unused
	 */
	if (!(isRoot && hasVoidPage))
	{
		UnlockReleaseBuffer(buffer);
	}
	else
	{
		Assert(rootBuffer);
		*rootBuffer = buffer;
	}

	return hasVoidPage;
}

/*
 * Delete a posting tree page.
 */
static void
vodkaDeletePage(VodkaVacuumState *gvs, BlockNumber deleteBlkno, BlockNumber leftBlkno,
			  BlockNumber parentBlkno, OffsetNumber myoff, bool isParentRoot)
{
	Buffer		dBuffer;
	Buffer		lBuffer;
	Buffer		pBuffer;
	Page		page,
				parentPage;
	BlockNumber	rightlink;

	/*
	 * Lock the pages in the same order as an insertion would, to avoid
	 * deadlocks: left, then right, then parent.
	 */
	lBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, leftBlkno,
								 RBM_NORMAL, gvs->strategy);
	dBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, deleteBlkno,
								 RBM_NORMAL, gvs->strategy);
	pBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, parentBlkno,
								 RBM_NORMAL, gvs->strategy);

	LockBuffer(lBuffer, VODKA_EXCLUSIVE);
	LockBuffer(dBuffer, VODKA_EXCLUSIVE);
	if (!isParentRoot)			/* parent is already locked by
								 * LockBufferForCleanup() */
		LockBuffer(pBuffer, VODKA_EXCLUSIVE);

	START_CRIT_SECTION();

	/* Unlink the page by chanvodkag left sibling's rightlink */
	page = BufferGetPage(dBuffer);
	rightlink = VodkaPageGetOpaque(page)->rightlink;

	page = BufferGetPage(lBuffer);
	VodkaPageGetOpaque(page)->rightlink = rightlink;

	/* Delete downlink from parent */
	parentPage = BufferGetPage(pBuffer);
#ifdef USE_ASSERT_CHECKING
	do
	{
		PostingItem *tod = VodkaDataPageGetPostingItem(parentPage, myoff);

		Assert(PostingItemGetBlockNumber(tod) == deleteBlkno);
	} while (0);
#endif
	VodkaPageDeletePostingItem(parentPage, myoff);

	page = BufferGetPage(dBuffer);

	/*
	 * we shouldn't change rightlink field to save workability of running
	 * search scan
	 */
	VodkaPageGetOpaque(page)->flags = VODKA_DELETED;

	MarkBufferDirty(pBuffer);
	if (leftBlkno != InvalidBlockNumber)
		MarkBufferDirty(lBuffer);
	MarkBufferDirty(dBuffer);

	if (RelationNeedsWAL(gvs->index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[4];
		vodkaxlogDeletePage data;
		int			n;

		data.node = gvs->index->rd_node;
		data.blkno = deleteBlkno;
		data.parentBlkno = parentBlkno;
		data.parentOffset = myoff;
		data.leftBlkno = leftBlkno;
		data.rightLink = VodkaPageGetOpaque(page)->rightlink;

		rdata[0].buffer = dBuffer;
		rdata[0].buffer_std = FALSE;
		rdata[0].data = NULL;
		rdata[0].len = 0;
		rdata[0].next = rdata + 1;

		rdata[1].buffer = pBuffer;
		rdata[1].buffer_std = FALSE;
		rdata[1].data = NULL;
		rdata[1].len = 0;
		rdata[1].next = rdata + 2;

		if (leftBlkno != InvalidBlockNumber)
		{
			rdata[2].buffer = lBuffer;
			rdata[2].buffer_std = FALSE;
			rdata[2].data = NULL;
			rdata[2].len = 0;
			rdata[2].next = rdata + 3;
			n = 3;
		}
		else
			n = 2;

		rdata[n].buffer = InvalidBuffer;
		rdata[n].buffer_std = FALSE;
		rdata[n].len = sizeof(vodkaxlogDeletePage);
		rdata[n].data = (char *) &data;
		rdata[n].next = NULL;

		recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_DELETE_PAGE, rdata);
		PageSetLSN(page, recptr);
		PageSetLSN(parentPage, recptr);
		if (leftBlkno != InvalidBlockNumber)
		{
			page = BufferGetPage(lBuffer);
			PageSetLSN(page, recptr);
		}
	}

	if (!isParentRoot)
		LockBuffer(pBuffer, VODKA_UNLOCK);
	ReleaseBuffer(pBuffer);
	UnlockReleaseBuffer(lBuffer);
	UnlockReleaseBuffer(dBuffer);

	END_CRIT_SECTION();

	gvs->result->pages_deleted++;
}

typedef struct DataPageDeleteStack
{
	struct DataPageDeleteStack *child;
	struct DataPageDeleteStack *parent;

	BlockNumber blkno;			/* current block number */
	BlockNumber leftBlkno;		/* rightest non-deleted page on left */
	bool		isRoot;
} DataPageDeleteStack;

/*
 * scans posting tree and deletes empty pages
 */
static bool
vodkaScanToDelete(VodkaVacuumState *gvs, BlockNumber blkno, bool isRoot,
				DataPageDeleteStack *parent, OffsetNumber myoff)
{
	DataPageDeleteStack *me;
	Buffer		buffer;
	Page		page;
	bool		meDelete = FALSE;
	bool		isempty;

	if (isRoot)
	{
		me = parent;
	}
	else
	{
		if (!parent->child)
		{
			me = (DataPageDeleteStack *) palloc0(sizeof(DataPageDeleteStack));
			me->parent = parent;
			parent->child = me;
			me->leftBlkno = InvalidBlockNumber;
		}
		else
			me = parent->child;
	}

	buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, gvs->strategy);
	page = BufferGetPage(buffer);

	Assert(VodkaPageIsData(page));

	if (!VodkaPageIsLeaf(page))
	{
		OffsetNumber i;

		me->blkno = blkno;
		for (i = FirstOffsetNumber; i <= VodkaPageGetOpaque(page)->maxoff; i++)
		{
			PostingItem *pitem = VodkaDataPageGetPostingItem(page, i);

			if (vodkaScanToDelete(gvs, PostingItemGetBlockNumber(pitem), FALSE, me, i))
				i--;
		}
	}

	if (VodkaPageIsLeaf(page))
		isempty = VodkaDataLeafPageIsEmpty(page);
	else
		isempty = VodkaPageGetOpaque(page)->maxoff < FirstOffsetNumber;

	if (isempty)
	{
		/* we never delete the left- or rightmost branch */
		if (me->leftBlkno != InvalidBlockNumber && !VodkaPageRightMost(page))
		{
			Assert(!isRoot);
			vodkaDeletePage(gvs, blkno, me->leftBlkno, me->parent->blkno, myoff, me->parent->isRoot);
			meDelete = TRUE;
		}
	}

	ReleaseBuffer(buffer);

	if (!meDelete)
		me->leftBlkno = blkno;

	return meDelete;
}

static void
vodkaVacuumPostingTree(VodkaVacuumState *gvs, BlockNumber rootBlkno)
{
	Buffer		rootBuffer = InvalidBuffer;
	DataPageDeleteStack root,
			   *ptr,
			   *tmp;

	if (vodkaVacuumPostingTreeLeaves(gvs, rootBlkno, TRUE, &rootBuffer) == FALSE)
	{
		Assert(rootBuffer == InvalidBuffer);
		return;
	}

	memset(&root, 0, sizeof(DataPageDeleteStack));
	root.leftBlkno = InvalidBlockNumber;
	root.isRoot = TRUE;

	vacuum_delay_point();

	vodkaScanToDelete(gvs, rootBlkno, TRUE, &root, InvalidOffsetNumber);

	ptr = root.child;
	while (ptr)
	{
		tmp = ptr->child;
		pfree(ptr);
		ptr = tmp;
	}

	UnlockReleaseBuffer(rootBuffer);
}

/*
 * returns modified page or NULL if page isn't modified.
 * Function works with orivodkaal page until first change is occurred,
 * then page is copied into temporary one.
 */
static Page
vodkaVacuumEntryPage(VodkaVacuumState *gvs, Buffer buffer, BlockNumber *roots, uint32 *nroot)
{
	Page		origpage = BufferGetPage(buffer),
				tmppage;
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(origpage);

	tmppage = origpage;

	*nroot = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));

		if (VodkaIsPostingTree(itup))
		{
			/*
			 * store posting tree's roots for further processing, we can't
			 * vacuum it just now due to risk of deadlocks with scans/inserts
			 */
			roots[*nroot] = VodkaGetDownlink(itup);
			(*nroot)++;
		}
		else if (VodkaGetNPosting(itup) > 0)
		{
			int			nitems;
			ItemPointer uncompressed;

			/*
			 * Vacuum posting list with proper function for compressed and
			 * uncompressed format.
			 */
			if (VodkaItupIsCompressed(itup))
				uncompressed = vodkaPostingListDecode((VodkaPostingList *) VodkaGetPosting(itup), &nitems);
			else
			{
				uncompressed = (ItemPointer) VodkaGetPosting(itup);
				nitems = VodkaGetNPosting(itup);
			}

			uncompressed = vodkaVacuumItemPointers(gvs, uncompressed, nitems,
												 &nitems);
			if (uncompressed)
			{
				/*
				 * Some ItemPointers were deleted, recreate tuple.
				 */
				OffsetNumber attnum;
				Datum		key;
				VodkaNullCategory category;
				VodkaPostingList *plist;
				int			plistsize;

				if (nitems > 0)
				{
					plist = vodkaCompressPostingList(uncompressed, nitems, VodkaMaxItemSize, NULL);
					plistsize = SizeOfVodkaPostingList(plist);
				}
				else
				{
					plist = NULL;
					plistsize = 0;
				}

				/*
				 * if we already created a temporary page, make changes in place
				 */
				if (tmppage == origpage)
				{
					/*
					 * On first difference, create a temporary copy of the
					 * page and copy the tuple's posting list to it.
					 */
					tmppage = PageGetTempPageCopy(origpage);

					/* set itup pointer to new page */
					itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));
				}

				attnum = vodkatuple_get_attrnum(&gvs->vodkastate, itup);
				key = vodkatuple_get_key(&gvs->vodkastate, itup, &category);
				itup = VodkaFormTuple(&gvs->vodkastate, attnum, key, category,
									(char *) plist, plistsize,
									nitems, true);
				if (plist)
					pfree(plist);
				PageIndexTupleDelete(tmppage, i);

				if (PageAddItem(tmppage, (Item) itup, IndexTupleSize(itup), i, false, false) != i)
					elog(ERROR, "failed to add item to index page in \"%s\"",
						 RelationGetRelationName(gvs->index));

				pfree(itup);
			}
		}
	}

	return (tmppage == origpage) ? NULL : tmppage;
}

Datum
vodkabulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
	Relation	index = info->index;
	BlockNumber blkno = VODKA_ROOT_BLKNO;
	VodkaVacuumState gvs;
	Buffer		buffer;
	BlockNumber rootOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
	uint32		nRoot;

	gvs.tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
									   "Vodka vacuum temporary context",
									   ALLOCSET_DEFAULT_MINSIZE,
									   ALLOCSET_DEFAULT_INITSIZE,
									   ALLOCSET_DEFAULT_MAXSIZE);
	gvs.index = index;
	gvs.callback = callback;
	gvs.callback_state = callback_state;
	gvs.strategy = info->strategy;
	initVodkaState(&gvs.vodkastate, index);

	/* first time through? */
	if (stats == NULL)
	{
		/* Yes, so initialize stats to zeroes */
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		/* and cleanup any pending inserts */
		vodkaInsertCleanup(&gvs.vodkastate, true, stats);
	}

	/* we'll re-count the tuples each time */
	stats->num_index_tuples = 0;
	gvs.result = stats;

	buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, info->strategy);

	/* find leaf page */
	for (;;)
	{
		Page		page = BufferGetPage(buffer);
		IndexTuple	itup;

		LockBuffer(buffer, VODKA_SHARE);

		Assert(!VodkaPageIsData(page));

		if (VodkaPageIsLeaf(page))
		{
			LockBuffer(buffer, VODKA_UNLOCK);
			LockBuffer(buffer, VODKA_EXCLUSIVE);

			if (blkno == VODKA_ROOT_BLKNO && !VodkaPageIsLeaf(page))
			{
				LockBuffer(buffer, VODKA_UNLOCK);
				continue;		/* check it one more */
			}
			break;
		}

		Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
		blkno = VodkaGetDownlink(itup);
		Assert(blkno != InvalidBlockNumber);

		UnlockReleaseBuffer(buffer);
		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
	}

	/* right now we found leftmost page in entry's BTree */

	for (;;)
	{
		Page		page = BufferGetPage(buffer);
		Page		resPage;
		uint32		i;

		Assert(!VodkaPageIsData(page));

		resPage = vodkaVacuumEntryPage(&gvs, buffer, rootOfPostingTree, &nRoot);

		blkno = VodkaPageGetOpaque(page)->rightlink;

		if (resPage)
		{
			START_CRIT_SECTION();
			PageRestoreTempPage(resPage, page);
			MarkBufferDirty(buffer);
			xlogVacuumPage(gvs.index, buffer);
			UnlockReleaseBuffer(buffer);
			END_CRIT_SECTION();
		}
		else
		{
			UnlockReleaseBuffer(buffer);
		}

		vacuum_delay_point();

		for (i = 0; i < nRoot; i++)
		{
			vodkaVacuumPostingTree(&gvs, rootOfPostingTree[i]);
			vacuum_delay_point();
		}

		if (blkno == InvalidBlockNumber)		/* rightmost page */
			break;

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, VODKA_EXCLUSIVE);
	}

	freeVodkaState(&gvs.vodkastate);
	MemoryContextDelete(gvs.tmpCxt);

	PG_RETURN_POINTER(gvs.result);
}

Datum
vodkavacuumcleanup(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation	index = info->index;
	bool		needLock;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	VodkaState	vodkastate;
	VodkaStatsData idxStat;

	/*
	 * In an autovacuum analyze, we want to clean up pending insertions.
	 * Otherwise, an ANALYZE-only call is a no-op.
	 */
	if (info->analyze_only)
	{
		if (IsAutoVacuumWorkerProcess())
		{
			initVodkaState(&vodkastate, index);
			vodkaInsertCleanup(&vodkastate, true, stats);
			freeVodkaState(&vodkastate);
		}
		PG_RETURN_POINTER(stats);
	}

	/*
	 * Set up all-zero stats and cleanup pending inserts if vodkabulkdelete
	 * wasn't called
	 */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		initVodkaState(&vodkastate, index);
		vodkaInsertCleanup(&vodkastate, true, stats);
		freeVodkaState(&vodkastate);
	}

	memset(&idxStat, 0, sizeof(idxStat));

	/*
	 * XXX we always report the heap tuple count as the number of index
	 * entries.  This is bogus if the index is partial, but it's real hard to
	 * tell how many distinct heap entries are referenced by a VODKA index.
	 */
	stats->num_index_tuples = info->num_heap_tuples;
	stats->estimated_count = info->estimated_count;

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	totFreePages = 0;

	for (blkno = VODKA_ROOT_BLKNO; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, VODKA_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (VodkaPageIsDeleted(page))
		{
			Assert(blkno != VODKA_ROOT_BLKNO);
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else if (VodkaPageIsData(page))
		{
			idxStat.nDataPages++;
		}
		else if (!VodkaPageIsList(page))
		{
			idxStat.nEntryPages++;

			if (VodkaPageIsLeaf(page))
				idxStat.nEntries += PageGetMaxOffsetNumber(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	/* Update the metapage with accurate page and entry counts */
	idxStat.nTotalPages = npages;
	vodkaUpdateStats(info->index, &idxStat);

	/* Finally, vacuum the FSM */
	IndexFreeSpaceMapVacuum(info->index);

	stats->pages_free = totFreePages;

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	PG_RETURN_POINTER(stats);
}
