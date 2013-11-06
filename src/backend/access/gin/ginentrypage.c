/*-------------------------------------------------------------------------
 *
 * ginentrypage.c
 *	  page utilities routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginentrypage.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "utils/rel.h"

/*
 * Read item pointers from leaf data page. Information is stored in the same
 * manner as in leaf data pages.
 */
void
ginReadTuple(GinState *ginstate, OffsetNumber attnum,
			 IndexTuple itup, ItemPointerData *ipd)
{
	Pointer		ptr;
	int			nipd = GinGetNPosting(itup), i;
	ItemPointerData ip = {{0,0},0};

	ptr = GinGetPosting(itup);

	if (GinItupIsCompressed(itup))
	{
		for (i = 0; i < nipd; i++)
		{
			ptr = ginDataPageLeafReadItemPointer(ptr, &ip);
			ipd[i] = ip;
		}
	}
	else
	{
		memcpy(ipd, ptr, sizeof(ItemPointerData) * nipd);
	}
}

/*
 * Form a non-leaf entry tuple by copying the key data from the given tuple,
 * which can be either a leaf or non-leaf entry tuple.
 *
 * Any posting list in the source tuple is not copied.	The specified child
 * block number is inserted into t_tid.
 */
static IndexTuple
GinFormInteriorTuple(IndexTuple itup, Page page, BlockNumber childblk)
{
	IndexTuple	nitup;

	if (GinPageIsLeaf(page) && !GinIsPostingTree(itup))
	{
		/* Tuple contains a posting list, just copy stuff before that */
		uint32		origsize = GinGetPostingOffset(itup);

		origsize = MAXALIGN(origsize);
		nitup = (IndexTuple) palloc(origsize);
		memcpy(nitup, itup, origsize);
		/* ... be sure to fix the size header field ... */
		nitup->t_info &= ~INDEX_SIZE_MASK;
		nitup->t_info |= origsize;
	}
	else
	{
		/* Copy the tuple as-is */
		nitup = (IndexTuple) palloc(IndexTupleSize(itup));
		memcpy(nitup, itup, IndexTupleSize(itup));
	}

	/* Now insert the correct downlink */
	GinSetDownlink(nitup, childblk);

	return nitup;
}

/*
 * Entry tree is a "static", ie tuple never deletes from it,
 * so we don't use right bound, we use rightmost key instead.
 */
static IndexTuple
getRightMostTuple(Page page)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

	return (IndexTuple) PageGetItem(page, PageGetItemId(page, maxoff));
}

static bool
entryIsMoveRight(GinBtree btree, Page page)
{
	IndexTuple	itup;
	OffsetNumber attnum;
	Datum		key;
	GinNullCategory category;

	if (GinPageRightMost(page))
		return FALSE;

	itup = getRightMostTuple(page);
	attnum = gintuple_get_attrnum(btree->ginstate, itup);
	key = gintuple_get_key(btree->ginstate, itup, &category);

	if (ginCompareAttEntries(btree->ginstate,
				   btree->entryAttnum, btree->entryKey, btree->entryCategory,
							 attnum, key, category) > 0)
		return TRUE;

	return FALSE;
}

/*
 * Find correct tuple in non-leaf page. It supposed that
 * page correctly chosen and searching value SHOULD be on page
 */
static BlockNumber
entryLocateEntry(GinBtree btree, GinBtreeStack *stack)
{
	OffsetNumber low,
				high,
				maxoff;
	IndexTuple	itup = NULL;
	int			result;
	Page		page = BufferGetPage(stack->buffer);

	Assert(!GinPageIsLeaf(page));
	Assert(!GinPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		stack->predictNumber *= PageGetMaxOffsetNumber(page);
		return btree->getLeftMostPage(btree, page);
	}

	low = FirstOffsetNumber;
	maxoff = high = PageGetMaxOffsetNumber(page);
	Assert(high >= low);

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		if (mid == maxoff && GinPageRightMost(page))
		{
			/* Right infinity */
			result = -1;
		}
		else
		{
			OffsetNumber attnum;
			Datum		key;
			GinNullCategory category;

			itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
			attnum = gintuple_get_attrnum(btree->ginstate, itup);
			key = gintuple_get_key(btree->ginstate, itup, &category);
			result = ginCompareAttEntries(btree->ginstate,
										  btree->entryAttnum,
										  btree->entryKey,
										  btree->entryCategory,
										  attnum, key, category);
		}

		if (result == 0)
		{
			stack->off = mid;
			Assert(GinGetDownlink(itup) != GIN_ROOT_BLKNO);
			return GinGetDownlink(itup);
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	Assert(high >= FirstOffsetNumber && high <= maxoff);

	stack->off = high;
	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, high));
	Assert(GinGetDownlink(itup) != GIN_ROOT_BLKNO);
	return GinGetDownlink(itup);
}

/*
 * Searches correct position for value on leaf page.
 * Page should be correctly chosen.
 * Returns true if value found on page.
 */
static bool
entryLocateLeafEntry(GinBtree btree, GinBtreeStack *stack)
{
	Page		page = BufferGetPage(stack->buffer);
	OffsetNumber low,
				high;

	Assert(GinPageIsLeaf(page));
	Assert(!GinPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		return TRUE;
	}

	low = FirstOffsetNumber;
	high = PageGetMaxOffsetNumber(page);

	if (high < low)
	{
		stack->off = FirstOffsetNumber;
		return false;
	}

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		IndexTuple	itup;
		OffsetNumber attnum;
		Datum		key;
		GinNullCategory category;
		int			result;

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
		attnum = gintuple_get_attrnum(btree->ginstate, itup);
		key = gintuple_get_key(btree->ginstate, itup, &category);
		result = ginCompareAttEntries(btree->ginstate,
									  btree->entryAttnum,
									  btree->entryKey,
									  btree->entryCategory,
									  attnum, key, category);
		if (result == 0)
		{
			stack->off = mid;
			return true;
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	stack->off = high;
	return false;
}

static OffsetNumber
entryFindChildPtr(GinBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
{
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(page);
	IndexTuple	itup;

	Assert(!GinPageIsLeaf(page));
	Assert(!GinPageIsData(page));

	/* if page isn't changed, we returns storedOff */
	if (storedOff >= FirstOffsetNumber && storedOff <= maxoff)
	{
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, storedOff));
		if (GinGetDownlink(itup) == blkno)
			return storedOff;

		/*
		 * we hope, that needed pointer goes to right. It's true if there
		 * wasn't a deletion
		 */
		for (i = storedOff + 1; i <= maxoff; i++)
		{
			itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
			if (GinGetDownlink(itup) == blkno)
				return i;
		}
		maxoff = storedOff - 1;
	}

	/* last chance */
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
		if (GinGetDownlink(itup) == blkno)
			return i;
	}

	return InvalidOffsetNumber;
}

static BlockNumber
entryGetLeftMostPage(GinBtree btree, Page page)
{
	IndexTuple	itup;

	Assert(!GinPageIsLeaf(page));
	Assert(!GinPageIsData(page));
	Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
	return GinGetDownlink(itup);
}

static bool
entryIsEnoughSpace(GinBtree btree, Buffer buf, OffsetNumber off)
{
	Size		itupsz = 0;
	Page		page = BufferGetPage(buf);

	Assert(btree->entry);
	Assert(!GinPageIsData(page));

	if (btree->isDelete)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

		itupsz = MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
	}

	if (PageGetFreeSpace(page) + itupsz >= MAXALIGN(IndexTupleSize(btree->entry)) + sizeof(ItemIdData))
		return true;

	return false;
}

/*
 * Delete tuple on leaf page if tuples existed and we
 * should update it, update old child blkno to new right page
 * if child split occurred
 */
static BlockNumber
entryPreparePage(GinBtree btree, Page page, OffsetNumber off)
{
	BlockNumber ret = InvalidBlockNumber;

	Assert(btree->entry);
	Assert(!GinPageIsData(page));

	if (btree->isDelete)
	{
		Assert(GinPageIsLeaf(page));
		PageIndexTupleDelete(page, off);
	}

	if (!GinPageIsLeaf(page) && btree->rightblkno != InvalidBlockNumber)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

		GinSetDownlink(itup, btree->rightblkno);
		ret = btree->rightblkno;
	}

	btree->rightblkno = InvalidBlockNumber;

	return ret;
}

/*
 * Place tuple on page and fills WAL record
 *
 * If the tuple doesn't fit, returns false without modifying the page.
 */
static bool
entryPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
				 XLogRecData **prdata)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber placed, off = stack->off;
	int			cnt = 0;

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[3];
	static ginxlogInsertEntry data;

	/* quick exit if it doesn't fit */
	if (!entryIsEnoughSpace(btree, buf, stack->off))
		return false;

	/* quick exit if it doesn't fit */
	if (!entryIsEnoughSpace(btree, buf, off))
		return false;

	*prdata = rdata;
	data.common.updateBlkno = entryPreparePage(btree, page, off);

	placed = PageAddItem(page, (Item) btree->entry, IndexTupleSize(btree->entry), off, false, false);
	if (placed != off)
		elog(ERROR, "failed to add item to index page in \"%s\"",
			 RelationGetRelationName(btree->index));

	data.common.node = btree->index->rd_node;
	data.common.blkno = BufferGetBlockNumber(buf);
	data.common.isData = FALSE;
	data.common.isLeaf = GinPageIsLeaf(page) ? TRUE : FALSE;
	data.isDelete = btree->isDelete;
	data.offset = off;

	/*
	 * Prevent full page write if child's split occurs. That is needed to
	 * remove incomplete splits while replaying WAL
	 *
	 * data.updateBlkno contains new block number (of newly created right
	 * page) for recently splited page.
	 */
	if (data.common.updateBlkno == InvalidBlockNumber)
	{
		rdata[0].buffer = buf;
		rdata[0].buffer_std = TRUE;
		rdata[0].data = NULL;
		rdata[0].len = 0;
		rdata[0].next = &rdata[1];
		cnt++;
	}

	rdata[cnt].buffer = InvalidBuffer;
	rdata[cnt].data = (char *) &data;
	rdata[cnt].len = offsetof(ginxlogInsertEntry, tuple);
	rdata[cnt].next = &rdata[cnt + 1];
	cnt++;

	rdata[cnt].buffer = InvalidBuffer;
	rdata[cnt].data = (char *) btree->entry;
	rdata[cnt].len = IndexTupleSize(btree->entry);
	rdata[cnt].next = NULL;

	btree->entry = NULL;

	return true;
}

/*
 * Place tuple and split page, original buffer(lbuf) leaves untouched,
 * returns shadow page of lbuf filled new data.
 * Tuples are distributed between pages by equal size on its, not
 * an equal number!
 */
static Page
entrySplitPage(GinBtree btree, Buffer lbuf, Buffer rbuf, OffsetNumber off, XLogRecData **prdata)
{
	OffsetNumber i,
				maxoff,
				separator = InvalidOffsetNumber;
	Size		totalsize = 0;
	Size		lsize = 0,
				size;
	char	   *ptr;
	IndexTuple	itup,
				leftrightmost = NULL;
	Page		page;
	Page		lpage = PageGetTempPageCopy(BufferGetPage(lbuf));
	Page		rpage = BufferGetPage(rbuf);
	Size		pageSize = PageGetPageSize(lpage);

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[2];
	static ginxlogSplit data;
	static char tupstore[2 * BLCKSZ];

	*prdata = rdata;
	data.leftChildBlkno = (GinPageIsLeaf(lpage)) ?
		InvalidOffsetNumber : GinGetDownlink(btree->entry);
	data.updateBlkno = entryPreparePage(btree, lpage, off);

	maxoff = PageGetMaxOffsetNumber(lpage);
	ptr = tupstore;

	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		if (i == off)
		{
			size = MAXALIGN(IndexTupleSize(btree->entry));
			memcpy(ptr, btree->entry, size);
			ptr += size;
			totalsize += size + sizeof(ItemIdData);
		}

		itup = (IndexTuple) PageGetItem(lpage, PageGetItemId(lpage, i));
		size = MAXALIGN(IndexTupleSize(itup));
		memcpy(ptr, itup, size);
		ptr += size;
		totalsize += size + sizeof(ItemIdData);
	}

	if (off == maxoff + 1)
	{
		size = MAXALIGN(IndexTupleSize(btree->entry));
		memcpy(ptr, btree->entry, size);
		ptr += size;
		totalsize += size + sizeof(ItemIdData);
	}

	GinInitPage(rpage, GinPageGetOpaque(lpage)->flags, pageSize);
	GinInitPage(lpage, GinPageGetOpaque(rpage)->flags, pageSize);

	ptr = tupstore;
	maxoff++;
	lsize = 0;

	page = lpage;
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		itup = (IndexTuple) ptr;

		if (lsize > totalsize / 2)
		{
			if (separator == InvalidOffsetNumber)
				separator = i - 1;
			page = rpage;
		}
		else
		{
			leftrightmost = itup;
			lsize += MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
		}

		if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to index page in \"%s\"",
				 RelationGetRelationName(btree->index));
		ptr += MAXALIGN(IndexTupleSize(itup));
	}

	btree->entry = GinFormInteriorTuple(leftrightmost, lpage,
										BufferGetBlockNumber(lbuf));

	btree->rightblkno = BufferGetBlockNumber(rbuf);

	data.node = btree->index->rd_node;
	data.rootBlkno = InvalidBlockNumber;
	data.lblkno = BufferGetBlockNumber(lbuf);
	data.rblkno = BufferGetBlockNumber(rbuf);
	data.separator = separator;
	data.nitem = maxoff;
	data.isData = FALSE;
	data.isLeaf = GinPageIsLeaf(lpage) ? TRUE : FALSE;
	data.isRootSplit = FALSE;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(ginxlogSplit);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = tupstore;
	rdata[1].len = MAXALIGN(totalsize);
	rdata[1].next = NULL;

	return lpage;
}

/*
 * return newly allocated rightmost tuple
 */
IndexTuple
ginPageGetLinkItup(Buffer buf)
{
	IndexTuple	itup,
				nitup;
	Page		page = BufferGetPage(buf);

	itup = getRightMostTuple(page);
	nitup = GinFormInteriorTuple(itup, page, BufferGetBlockNumber(buf));

	return nitup;
}

/*
 * Fills new root by rightest values from child.
 * Also called from ginxlog, should not use btree
 */
void
ginEntryFillRoot(GinBtree btree, Buffer root, Buffer lbuf, Buffer rbuf)
{
	Page		page;
	IndexTuple	itup;

	page = BufferGetPage(root);

	itup = ginPageGetLinkItup(lbuf);
	if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to index root page");
	pfree(itup);

	itup = ginPageGetLinkItup(rbuf);
	if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to index root page");
	pfree(itup);
}

/*
 * Set up GinBtree for entry page access
 *
 * Note: during WAL recovery, there may be no valid data in ginstate
 * other than a faked-up Relation pointer; the key datum is bogus too.
 */
void
ginPrepareEntryScan(GinBtree btree, OffsetNumber attnum,
					Datum key, GinNullCategory category,
					GinState *ginstate)
{
	memset(btree, 0, sizeof(GinBtreeData));

	btree->index = ginstate->index;
	btree->ginstate = ginstate;

	btree->findChildPage = entryLocateEntry;
	btree->isMoveRight = entryIsMoveRight;
	btree->findItem = entryLocateLeafEntry;
	btree->findChildPtr = entryFindChildPtr;
	btree->getLeftMostPage = entryGetLeftMostPage;
	btree->placeToPage = entryPlaceToPage;
	btree->splitPage = entrySplitPage;
	btree->fillRoot = ginEntryFillRoot;

	btree->isData = FALSE;
	btree->searchMode = FALSE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;

	btree->entryAttnum = attnum;
	btree->entryKey = key;
	btree->entryCategory = category;
	btree->isDelete = FALSE;
}
