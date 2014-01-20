/*-------------------------------------------------------------------------
 *
 * ginvacuum.c
 *	  delete & vacuum routines for the postgres GIN
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginvacuum.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

typedef struct
{
	Relation	index;
	IndexBulkDeleteResult *result;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	GinState	ginstate;
	BufferAccessStrategy strategy;
} GinVacuumState;


/*
 * Vacuums a compressed posting list.
 *
 * If there are no vacuumable items in the list, returns NULL. Otherwise
 * returns a new, re-encoded posting list with the vacuumable items removed.
 * In either case, the number of items remaining in the list is returned in
 * *nremaining.
 */
static GinPostingList *
ginVacuumPostingListCompressed(GinVacuumState *gvs, GinPostingList *orig,
							   int *nremaining)
{
	int			remaining = 0;
	int			i;
	ItemPointer	items;
	int			ndecoded;
	GinPostingList *result = NULL;

	items = ginPostingListDecode(orig, &ndecoded);

	/*
	 * iterate through the decoded posting list.
	 */
	for (i = 0; i < ndecoded; i++)
	{
		if (gvs->callback(&items[i], gvs->callback_state))
		{
			gvs->result->tuples_removed += 1;
		}
		else
		{
			items[remaining] = items[i];
			remaining++;
			gvs->result->num_index_tuples += 1;
		}
	}

	if (nremaining)
		*nremaining = remaining;

	if (remaining != ndecoded)
		result = ginCompressPostingList(items, remaining,
										SizeOfGinPostingList(orig), NULL);

	pfree(items);
	return result;
}

/*
 * Vacuums an uncompressed posting list. The size of the must can be specified
 * in number of items (nitems).
 *
 * If none of the items need to be removed, returns NULL. Otherwise returns
 * a new palloc'd array with the remaining items. The number of remaining
 * items is returned in *nremaining.
 */
static ItemPointer
ginVacuumPostingListUncompressed(GinVacuumState *gvs, ItemPointerData *items,
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
 * Create a WAL record for vacuuming leaf page.
 */
static void
xlogVacuumPage(Relation index, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	XLogRecPtr	recptr;
	XLogRecData rdata[3];
	ginxlogVacuumPage xlrec;
	uint16		lower;
	uint16		upper;

	/* This is only used for posting tree leaf pages. */
	Assert(GinPageIsData(page));
	Assert(GinPageIsLeaf(page));
	Assert(GinPageIsCompressed(page)); /* no pre-9.4 format pages either */

	if (!RelationNeedsWAL(index))
		return;

	xlrec.node = index->rd_node;
	xlrec.blkno = BufferGetBlockNumber(buffer);

	/* Assume we can omit data between pd_lower and pd_upper */
	lower = ((PageHeader) page)->pd_lower;
	upper = ((PageHeader) page)->pd_upper;

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

	rdata[0].buffer = buffer;
	rdata[0].buffer_std = TRUE;
	rdata[0].len = 0;
	rdata[0].data = NULL;
	rdata[0].next = rdata + 1;

	rdata[1].buffer = InvalidBuffer;
	rdata[1].len = sizeof(ginxlogVacuumPage);
	rdata[1].data = (char *) page;
	rdata[1].next = &rdata[2];

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

	recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_VACUUM_PAGE, rdata);
	PageSetLSN(page, recptr);
}

static bool
ginVacuumPostingTreeLeaves(GinVacuumState *gvs, BlockNumber blkno, bool isRoot, Buffer *rootBuffer)
{
	Buffer		buffer;
	Page		page;
	bool		hasVoidPage = FALSE;

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
		LockBuffer(buffer, GIN_EXCLUSIVE);

	Assert(GinPageIsData(page));

	if (GinPageIsLeaf(page))
	{
		bool		removedsomething = false;
		ItemPointer uncompressed;
		ItemPointer cleaned;
		int			nuncompressed;
		int			uncompressedSize;
		GinPostingList *segment;
		List	   *compressedSegs = NIL;
		char	   *endptr;
		int			totalsize = 0;

		/* Vacuum the uncompressed items */
		uncompressed = GinDataLeafPageGetUncompressed(page, &nuncompressed);
		cleaned = ginVacuumPostingListUncompressed(gvs,
												   uncompressed,
												   nuncompressed,
												   &nuncompressed);
		if (cleaned)
		{
			removedsomething = true;
			uncompressed = cleaned;
		}

		uncompressedSize = nuncompressed * sizeof(ItemPointerData);
		totalsize += uncompressedSize;

		/* Vacuum the compressed segments */
		if (GinPageIsCompressed(page))
		{
			segment = GinDataLeafPageGetPostingList(page);
			endptr = ((char *) segment) + GinDataLeafPageGetPostingListSize(page);
			while ((char *) segment < endptr)
			{
				int			segsize = SizeOfGinPostingList(segment);
				GinPostingList *vacuumed;

				vacuumed = ginVacuumPostingListCompressed(gvs, segment, NULL);
				if (vacuumed)
				{
					removedsomething = true;
				}
				else
				{
					vacuumed = (GinPostingList *) palloc(segsize);
					memcpy(vacuumed, segment, segsize);
				}
				/* Vacuum shouldn't increase size of segment */
				Assert(SizeOfGinPostingList(vacuumed) <= segsize);
				totalsize += segsize;
				compressedSegs = lappend(compressedSegs, vacuumed);

				segment = GinNextPostingListSegment(segment);
			}
			Assert((Pointer) segment == endptr);
		}

		/* XXX: now would be a good time to pack any uncompressed items */

		/* If we removed any items, reconstruct the page from the pieces */
		if (removedsomething)
		{
			PageHeader	phdr = (PageHeader) page;
			char	   *target;
			int			compressedsize;
			ListCell   *lc;

			if (totalsize - uncompressedSize > GinDataLeafPageGetPostingListSize(page))
			{
				/*
				 * shouldn't happen, because removing items never enlarges a
				 * compressed posting list.
				 */
				elog(ERROR, "could not fit all items on GIN posting tree page after vacuum");
			}

			START_CRIT_SECTION();

			/*
			 * We always write in the compressed format, even if the page was
			 * previously not compressed.
			 */
			GinPageSetCompressed(page);

			target = (char *) GinDataLeafPageGetPostingList(page);
			compressedsize = 0;
			foreach(lc, compressedSegs)
			{
				GinPostingList *compressed = lfirst(lc);
				int			len = SizeOfGinPostingList(compressed);
				memcpy(target, compressed, len);
				target += len;
				compressedsize += len;
			}
			GinDataLeafPageSetPostingListSize(page, compressedsize);

			/* Write the uncompressed items */
			phdr->pd_upper = phdr->pd_special - uncompressedSize;
			memcpy(page + phdr->pd_upper, uncompressed, uncompressedSize);

			Assert(phdr->pd_upper >= phdr->pd_lower);

			MarkBufferDirty(buffer);
			xlogVacuumPage(gvs->index, buffer);

			END_CRIT_SECTION();
		}
		/* if root is a leaf page, we don't desire further processing */
		if (!isRoot && totalsize == 0)
			hasVoidPage = TRUE;
	}
	else
	{
		OffsetNumber i;
		bool		isChildHasVoid = FALSE;

		for (i = FirstOffsetNumber; i <= GinPageGetOpaque(page)->maxoff; i++)
		{
			PostingItem *pitem = GinDataPageGetPostingItem(page, i);

			if (ginVacuumPostingTreeLeaves(gvs, PostingItemGetBlockNumber(pitem), FALSE, NULL))
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
ginDeletePage(GinVacuumState *gvs, BlockNumber deleteBlkno, BlockNumber leftBlkno,
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

	LockBuffer(lBuffer, GIN_EXCLUSIVE);
	LockBuffer(dBuffer, GIN_EXCLUSIVE);
	if (!isParentRoot)			/* parent is already locked by
								 * LockBufferForCleanup() */
		LockBuffer(pBuffer, GIN_EXCLUSIVE);

	START_CRIT_SECTION();

	/* Unlink the page by changing left sibling's rightlink */
	page = BufferGetPage(dBuffer);
	rightlink = GinPageGetOpaque(page)->rightlink;

	page = BufferGetPage(lBuffer);
	GinPageGetOpaque(page)->rightlink = rightlink;

	/* Delete downlink from parent */
	parentPage = BufferGetPage(pBuffer);
#ifdef USE_ASSERT_CHECKING
	do
	{
		PostingItem *tod = GinDataPageGetPostingItem(parentPage, myoff);

		Assert(PostingItemGetBlockNumber(tod) == deleteBlkno);
	} while (0);
#endif
	GinPageDeletePostingItem(parentPage, myoff);

	page = BufferGetPage(dBuffer);

	/*
	 * we shouldn't change rightlink field to save workability of running
	 * search scan
	 */
	GinPageGetOpaque(page)->flags = GIN_DELETED;

	MarkBufferDirty(pBuffer);
	if (leftBlkno != InvalidBlockNumber)
		MarkBufferDirty(lBuffer);
	MarkBufferDirty(dBuffer);

	if (RelationNeedsWAL(gvs->index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[4];
		ginxlogDeletePage data;
		int			n;

		data.node = gvs->index->rd_node;
		data.blkno = deleteBlkno;
		data.parentBlkno = parentBlkno;
		data.parentOffset = myoff;
		data.leftBlkno = leftBlkno;
		data.rightLink = GinPageGetOpaque(page)->rightlink;

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
		rdata[n].len = sizeof(ginxlogDeletePage);
		rdata[n].data = (char *) &data;
		rdata[n].next = NULL;

		recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_PAGE, rdata);
		PageSetLSN(page, recptr);
		PageSetLSN(parentPage, recptr);
		if (leftBlkno != InvalidBlockNumber)
		{
			page = BufferGetPage(lBuffer);
			PageSetLSN(page, recptr);
		}
	}

	if (!isParentRoot)
		LockBuffer(pBuffer, GIN_UNLOCK);
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
ginScanToDelete(GinVacuumState *gvs, BlockNumber blkno, bool isRoot,
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

	Assert(GinPageIsData(page));

	if (!GinPageIsLeaf(page))
	{
		OffsetNumber i;

		me->blkno = blkno;
		for (i = FirstOffsetNumber; i <= GinPageGetOpaque(page)->maxoff; i++)
		{
			PostingItem *pitem = GinDataPageGetPostingItem(page, i);

			if (ginScanToDelete(gvs, PostingItemGetBlockNumber(pitem), FALSE, me, i))
				i--;
		}
	}

	if (GinPageIsLeaf(page))
		isempty = (GinDataLeafPageGetPostingListSize(page) == 0);
	else
		isempty = GinPageGetOpaque(page)->maxoff < FirstOffsetNumber;

	if (isempty)
	{
		/* we never delete the left- or rightmost branch */
		if (me->leftBlkno != InvalidBlockNumber && !GinPageRightMost(page))
		{
			Assert(!isRoot);
			ginDeletePage(gvs, blkno, me->leftBlkno, me->parent->blkno, myoff, me->parent->isRoot);
			meDelete = TRUE;
		}
	}

	ReleaseBuffer(buffer);

	if (!meDelete)
		me->leftBlkno = blkno;

	return meDelete;
}

static void
ginVacuumPostingTree(GinVacuumState *gvs, BlockNumber rootBlkno)
{
	Buffer		rootBuffer = InvalidBuffer;
	DataPageDeleteStack root,
			   *ptr,
			   *tmp;

	if (ginVacuumPostingTreeLeaves(gvs, rootBlkno, TRUE, &rootBuffer) == FALSE)
	{
		Assert(rootBuffer == InvalidBuffer);
		return;
	}

	memset(&root, 0, sizeof(DataPageDeleteStack));
	root.leftBlkno = InvalidBlockNumber;
	root.isRoot = TRUE;

	vacuum_delay_point();

	ginScanToDelete(gvs, rootBlkno, TRUE, &root, InvalidOffsetNumber);

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
 * Function works with original page until first change is occurred,
 * then page is copied into temporary one.
 */
static Page
ginVacuumEntryPage(GinVacuumState *gvs, Buffer buffer, BlockNumber *roots, uint32 *nroot)
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

		if (GinIsPostingTree(itup))
		{
			/*
			 * store posting tree's roots for further processing, we can't
			 * vacuum it just now due to risk of deadlocks with scans/inserts
			 */
			roots[*nroot] = GinGetDownlink(itup);
			(*nroot)++;
		}
		else if (GinGetNPosting(itup) > 0)
		{
			/*
			 * if we already created a temporary page, make changes in place
			 */
			GinPostingList *cleaned;
			int			nitems;

			/*
			 * Vacuum posting list with proper function for compressed and
			 * uncompressed format.
			 */
			if (GinItupIsCompressed(itup))
				cleaned = ginVacuumPostingListCompressed(gvs,
														 (GinPostingList *) GinGetPosting(itup),
					&nitems);
			else
			{
				ItemPointer uncompressed;

				uncompressed = ginVacuumPostingListUncompressed(gvs,
											(ItemPointer)GinGetPosting(itup),
											GinGetNPosting(itup),
											&nitems);
				if (uncompressed)
					cleaned = ginCompressPostingList(uncompressed, nitems, 0, NULL);
				else
					cleaned = NULL;
			}

			if (cleaned)
			{
				OffsetNumber attnum;
				Datum		key;
				GinNullCategory category;

				/*
				 * Some ItemPointers were deleted, recreate tuple.
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

				attnum = gintuple_get_attrnum(&gvs->ginstate, itup);
				key = gintuple_get_key(&gvs->ginstate, itup, &category);
				/* FIXME */
				itup = GinFormTuple(&gvs->ginstate, attnum, key, category,
									(char *) cleaned,
									SizeOfGinPostingList(cleaned),
									nitems, true);
				pfree(cleaned);
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
ginbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
	Relation	index = info->index;
	BlockNumber blkno = GIN_ROOT_BLKNO;
	GinVacuumState gvs;
	Buffer		buffer;
	BlockNumber rootOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
	uint32		nRoot;

	gvs.index = index;
	gvs.callback = callback;
	gvs.callback_state = callback_state;
	gvs.strategy = info->strategy;
	initGinState(&gvs.ginstate, index);

	/* first time through? */
	if (stats == NULL)
	{
		/* Yes, so initialize stats to zeroes */
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		/* and cleanup any pending inserts */
		ginInsertCleanup(&gvs.ginstate, true, stats);
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

		LockBuffer(buffer, GIN_SHARE);

		Assert(!GinPageIsData(page));

		if (GinPageIsLeaf(page))
		{
			LockBuffer(buffer, GIN_UNLOCK);
			LockBuffer(buffer, GIN_EXCLUSIVE);

			if (blkno == GIN_ROOT_BLKNO && !GinPageIsLeaf(page))
			{
				LockBuffer(buffer, GIN_UNLOCK);
				continue;		/* check it one more */
			}
			break;
		}

		Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
		blkno = GinGetDownlink(itup);
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

		Assert(!GinPageIsData(page));

		resPage = ginVacuumEntryPage(&gvs, buffer, rootOfPostingTree, &nRoot);

		blkno = GinPageGetOpaque(page)->rightlink;

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
			ginVacuumPostingTree(&gvs, rootOfPostingTree[i]);
			vacuum_delay_point();
		}

		if (blkno == InvalidBlockNumber)		/* rightmost page */
			break;

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIN_EXCLUSIVE);
	}

	PG_RETURN_POINTER(gvs.result);
}

Datum
ginvacuumcleanup(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation	index = info->index;
	bool		needLock;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	GinState	ginstate;
	GinStatsData idxStat;

	/*
	 * In an autovacuum analyze, we want to clean up pending insertions.
	 * Otherwise, an ANALYZE-only call is a no-op.
	 */
	if (info->analyze_only)
	{
		if (IsAutoVacuumWorkerProcess())
		{
			initGinState(&ginstate, index);
			ginInsertCleanup(&ginstate, true, stats);
		}
		PG_RETURN_POINTER(stats);
	}

	/*
	 * Set up all-zero stats and cleanup pending inserts if ginbulkdelete
	 * wasn't called
	 */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		initGinState(&ginstate, index);
		ginInsertCleanup(&ginstate, true, stats);
	}

	memset(&idxStat, 0, sizeof(idxStat));

	/*
	 * XXX we always report the heap tuple count as the number of index
	 * entries.  This is bogus if the index is partial, but it's real hard to
	 * tell how many distinct heap entries are referenced by a GIN index.
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

	for (blkno = GIN_ROOT_BLKNO; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIN_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (GinPageIsDeleted(page))
		{
			Assert(blkno != GIN_ROOT_BLKNO);
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else if (GinPageIsData(page))
		{
			idxStat.nDataPages++;
		}
		else if (!GinPageIsList(page))
		{
			idxStat.nEntryPages++;

			if (GinPageIsLeaf(page))
				idxStat.nEntries += PageGetMaxOffsetNumber(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	/* Update the metapage with accurate page and entry counts */
	idxStat.nTotalPages = npages;
	ginUpdateStats(info->index, &idxStat);

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
