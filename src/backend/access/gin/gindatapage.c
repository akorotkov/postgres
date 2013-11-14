/*-------------------------------------------------------------------------
 *
 * gindatapage.c
 *	  page utilities routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/gindatapage.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "miscadmin.h"
#include "utils/rel.h"

/*
 * Initialize leaf page of posting tree. Reverse space for item indexes in
 * the end of page.
 */
static void
GinInitDataLeafPage(Page page)
{
	GinInitPage(page, GIN_DATA | GIN_LEAF | GIN_COMPRESSED, BLCKSZ);
	((PageHeader) page)->pd_upper -= sizeof(GinDataLeafItemIndex) * GinDataLeafIndexCount;
}

/*
 * Checks, should we move to right link...
 * Compares inserting itemp pointer with right bound of current page
 */
static bool
dataIsMoveRight(GinBtree btree, Page page)
{
	ItemPointer iptr = GinDataPageGetRightBound(page);

	if (GinPageRightMost(page))
		return FALSE;

	return (ginCompareItemPointers(btree->items + btree->curitem, iptr) > 0) ? TRUE : FALSE;
}

/*
 * Find correct PostingItem in non-leaf page. It supposed that page
 * correctly chosen and searching value SHOULD be on page
 */
static BlockNumber
dataLocateItem(GinBtree btree, GinBtreeStack *stack)
{
	OffsetNumber low,
				high,
				maxoff;
	PostingItem *pitem = NULL;
	int			result;
	Page		page = BufferGetPage(stack->buffer);

	Assert(!GinPageIsLeaf(page));
	Assert(GinPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		stack->predictNumber *= GinPageGetOpaque(page)->maxoff;
		return btree->getLeftMostPage(btree, page);
	}

	low = FirstOffsetNumber;
	maxoff = high = GinPageGetOpaque(page)->maxoff;
	Assert(high >= low);

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		pitem = GinDataPageGetPostingItem(page, mid);

		if (mid == maxoff)
		{
			/*
			 * Right infinity, page already correctly chosen with a help of
			 * dataIsMoveRight
			 */
			result = -1;
		}
		else
		{
			pitem = GinDataPageGetPostingItem(page, mid);
			result = ginCompareItemPointers(btree->items + btree->curitem, &(pitem->key));
		}

		if (result == 0)
		{
			stack->off = mid;
			return PostingItemGetBlockNumber(pitem);
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	Assert(high >= FirstOffsetNumber && high <= maxoff);

	stack->off = high;
	pitem = GinDataPageGetPostingItem(page, high);
	return PostingItemGetBlockNumber(pitem);
}

/*
 * Find item pointer in leaf data page. Returns true if given item pointer is
 * found and false if it's not. Sets offset and iptrOut to last item pointer
 * which is less than given one. Sets ptrOut ahead that item pointer.
 */
static bool
findInLeafPageCompressed(GinBtree btree, Page page,
			   ItemPointerData *iptrOut, Pointer *ptrOut)
{
	int			i;
	ItemPointerData iptr = {{0,0},0};
	int			cmp;
	Pointer		ptr;
	Pointer		endPtr;
	bool		result = false;
	GinDataLeafItemIndex *indexes = GinPageGetIndexes(page);

	ptr = GinDataLeafPageGetPostingList(page);
	endPtr = GinDataLeafPageGetPostingListEnd(page);

	/*
	 * First, search the leaf-item index at the end of page. That narrows the
	 * range we need to linearly scan.
	 */
	for (i = 0; i < GinDataLeafIndexCount; i++)
	{
		GinDataLeafItemIndex *index = &indexes[i];
		if (index->pageOffset == 0)
			break;

		Assert(index->pageOffset < GinDataLeafPageGetPostingListSize(page));

		cmp = ginCompareItemPointers(&index->iptr, &btree->items[btree->curitem]);
		if (cmp < 0)
		{
			ptr = GinDataLeafPageGetPostingList(page) + index->pageOffset;
			iptr = index->iptr;
		}
		else
		{
			endPtr = GinDataLeafPageGetPostingList(page) + index->pageOffset;
			break;
		}
	}

	/* Search page in [first, maxoff] range found by page index */
	while (ptr < endPtr)
	{
		Pointer prev_ptr = ptr;
		ItemPointerData prev_iptr = iptr;

		Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

		ptr = ginDataPageLeafReadItemPointer(ptr, &iptr);

		Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

		cmp = ginCompareItemPointers(btree->items + btree->curitem, &iptr);
		if (cmp == 0)
		{
			result = true;
			break;
		}
		if (cmp < 0)
		{
			iptr = prev_iptr;
			ptr = prev_ptr;
			result = false;
			break;
		}
	}

	Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

	*ptrOut = ptr;
	*iptrOut = iptr;
	return result;
}

/*
 * Find correct PostingItem in non-leaf page. It supposed that page
 * correctly chosen and searching value SHOULD be on page
 */
static OffsetNumber
findInLeafPageUncompressed(GinBtree btree, Page page, OffsetNumber *off)
{
	OffsetNumber low,
				high;
	int			result;

	Assert(GinPageIsLeaf(page));
	Assert(GinPageIsData(page));

	low = FirstOffsetNumber;
	high = GinPageGetOpaque(page)->maxoff;

	if (high < low)
	{
		*off = FirstOffsetNumber;
		return false;
	}

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		result = ginCompareItemPointers(btree->items + btree->curitem, GinDataPageGetItemPointer(page, mid));

		if (result == 0)
		{
			*off = mid;
			return true;
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	*off = high;
	return false;
}

/*
 * Searches correct position for value within leaf page.
 * Page should already be correctly chosen.
 * Returns true if value found on page.
 */
static bool
dataLocateLeafItem(GinBtree btree, GinBtreeStack *stack)
{
	Page			page = BufferGetPage(stack->buffer);
	bool			result;
	Pointer			ptr;

	Assert(GinPageIsData(page) && GinPageIsLeaf(page));

	if (btree->fullScan)
	{
		stack->off = InvalidOffsetNumber;
		stack->pageOffset = GinDataLeafPageGetPostingList(page) - page;
		return TRUE;
	}

	if (GinPageIsCompressed(page))
	{
		result = findInLeafPageCompressed(btree, page, &stack->iptr, &ptr);
		stack->off = InvalidOffsetNumber;
		stack->pageOffset = ptr - page;
	}
	else
	{
		result = findInLeafPageUncompressed(btree, page, &stack->off);
		stack->pageOffset = InvalidOffsetNumber;
	}
	return result;
}

/*
 * Finds links to blkno on non-leaf page, returns offset of PostingItem
 */
static OffsetNumber
dataFindChildPtr(GinBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
{
	OffsetNumber i,
				maxoff = GinPageGetOpaque(page)->maxoff;
	PostingItem *pitem;

	Assert(!GinPageIsLeaf(page));
	Assert(GinPageIsData(page));

	/* if page isn't changed, we return storedOff */
	if (storedOff >= FirstOffsetNumber && storedOff <= maxoff)
	{
		pitem = GinDataPageGetPostingItem(page, storedOff);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return storedOff;

		/*
		 * we hope, that needed pointer goes to right. It's true if there
		 * wasn't a deletion
		 */
		for (i = storedOff + 1; i <= maxoff; i++)
		{
			pitem = GinDataPageGetPostingItem(page, i);
			if (PostingItemGetBlockNumber(pitem) == blkno)
				return i;
		}

		maxoff = storedOff - 1;
	}

	/* last chance */
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		pitem = GinDataPageGetPostingItem(page, i);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return i;
	}

	return InvalidOffsetNumber;
}

/*
 * returns blkno of leftmost child
 */
static BlockNumber
dataGetLeftMostPage(GinBtree btree, Page page)
{
	PostingItem *pitem;

	Assert(!GinPageIsLeaf(page));
	Assert(GinPageIsData(page));
	Assert(GinPageGetOpaque(page)->maxoff >= FirstOffsetNumber);

	pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber);
	return PostingItemGetBlockNumber(pitem);
}

/*
 * add PostingItem to a non-leaf page.
 */
void
GinDataPageAddPostingItem(Page page, PostingItem *data, OffsetNumber offset)
{
	OffsetNumber maxoff = GinPageGetOpaque(page)->maxoff;
	char	   *ptr;

	Assert(!GinPageIsLeaf(page));

	if (offset == InvalidOffsetNumber)
	{
		ptr = (char *) GinDataPageGetPostingItem(page, maxoff + 1);
	}
	else
	{
		ptr = (char *) GinDataPageGetPostingItem(page, offset);
		if (offset != maxoff + 1)
			memmove(ptr + sizeof(PostingItem),
					ptr,
					(maxoff - offset + 1) * sizeof(PostingItem));
	}
	memcpy(ptr, data, sizeof(PostingItem));

	GinPageGetOpaque(page)->maxoff++;
}

/*
 * Deletes posting item from non-leaf page
 */
void
GinPageDeletePostingItem(Page page, OffsetNumber offset)
{
	OffsetNumber maxoff = GinPageGetOpaque(page)->maxoff;

	Assert(!GinPageIsLeaf(page));
	Assert(offset >= FirstOffsetNumber && offset <= maxoff);

	if (offset != maxoff)
		memmove(GinDataPageGetPostingItem(page, offset),
				GinDataPageGetPostingItem(page, offset + 1),
				sizeof(PostingItem) * (maxoff - offset));

	GinPageGetOpaque(page)->maxoff--;
}

/*
 * In case of previous split update old child blkno to new right page
 */
static BlockNumber
dataPrepareData(GinBtree btree, Page page, OffsetNumber off)
{
	BlockNumber ret = InvalidBlockNumber;

	Assert(GinPageIsData(page));
	Assert(!GinPageIsLeaf(page));

	if (btree->rightblkno != InvalidBlockNumber)
	{
		PostingItem *pitem = GinDataPageGetPostingItem(page, off);

		PostingItemSetBlockNumber(pitem, btree->rightblkno);
		ret = btree->rightblkno;

		btree->rightblkno = InvalidBlockNumber;
	}

	return ret;
}

/*
 * Threshold to split bin between page item indexes into two. It's twice large
 * as average bin size, so after split bin will have size about average.
 */
#define SPLIT_THRESHOLD (2 * GinDataLeafMaxPostingListSize / (GinDataLeafIndexCount + 1))
/*
 * Threshold to append new page item indexes is average bin size.
 */
#define APPEND_THRESHOLD (GinDataLeafMaxPostingListSize / (GinDataLeafIndexCount + 1))


/*
 * Incremental update of page item indexes. Item indexes starting at offset
 * 'off' to be shift by 'len' bytes.
 */
void
incrUpdateItemIndexes(Page page, int off, int len)
{
	GinDataLeafItemIndex *indexes = GinPageGetIndexes(page);
	int			i;
	int			targetbin;

	Assert(GinPageIsData(page) && GinPageIsLeaf(page));

	/*
	 * First, adjust the offsets in any existing bins. This is required
	 * for correctness.
	 */
	for (i = GinDataLeafIndexCount - 1; i >= 0; i--)
	{
		if (indexes[i].pageOffset == 0)
			continue;

		if (indexes[i].pageOffset < off)
			break;

		indexes[i].pageOffset += len;
		Assert(indexes[i].pageOffset < GinDataLeafPageGetPostingListSize(page));
	}
	targetbin = i + 1;

	/*
	 * The leaf item index is now consistent. Check if the bin the new tuples
	 * fell into became so large that it should be split to make it balanced.
	 */
	for (;;)
	{
		int			nextoff;
		int			prevoff;
		Pointer		beginptr;
		Pointer		ptr;
		ItemPointerData iptr;

		if (indexes[GinDataLeafIndexCount - 1].pageOffset != 0)
			break; /* all slots are in use, can't insert a new one */

		if (indexes[targetbin].pageOffset == 0)
			nextoff = GinDataLeafPageGetPostingListSize(page);
		else
			nextoff = indexes[targetbin].pageOffset;
		if (targetbin == 0)
		{
			prevoff = 0;
			MemSet(&iptr, 0, sizeof(ItemPointerData));
		}
		else
		{
			prevoff = indexes[targetbin - 1].pageOffset;
			iptr = indexes[targetbin - 1].iptr;
		}

		/*
		 * Is this bin large enough to split?
		 */
		if (nextoff - prevoff < SPLIT_THRESHOLD)
			break;

		/* Shift page item indexes to create a hole for a new one */
		memmove(&indexes[targetbin + 1], &indexes[targetbin],
				sizeof(GinDataLeafItemIndex) *
				(GinDataLeafIndexCount - targetbin - 1));

		/*
		 * Scan through the bin to find the split point. The bin is not
		 * split in the middle, but at the first APPEND_THRESHOLD limit.
		 * That might leave the right bin still larger than SPLIT_THRESHOLD.
		 * In that case we will loop back to split it further.
		 */
		beginptr = GinDataLeafPageGetPostingList(page) + prevoff;
		ptr = beginptr;
		while (ptr - beginptr < APPEND_THRESHOLD)
			ptr = ginDataPageLeafReadItemPointer(ptr, &iptr);

		/* Add new bin */
		indexes[targetbin].iptr = iptr;
		indexes[targetbin].pageOffset = ptr - GinDataLeafPageGetPostingList(page);
		Assert(indexes[targetbin].pageOffset < GinDataLeafPageGetPostingListSize(page));

		/* Loop to possibly further split the next bin. */
		targetbin++;
	}
}

/*
 * Convert uncompressed leaf posting tree page into compressed format. In some
 * rare cases compress could fail because varbyte encoded of large 32-bit
 * integer could be 5 bytes and we additionally reserve space for item indexes.
 * If conversion succeed function returns true. If conversion failed function
 * returns false and leave page untouched.
 */
bool
dataCompressLeafPage(Page page)
{
	char		pageCopy[BLCKSZ];
	OffsetNumber	i, maxoff;
	ItemPointerData prev_iptr;
	ItemPointer iptr;
	Pointer		ptr, cur;
	Size		size = 0;

	/* Check if we've enough of space to store compressed representation */
	maxoff = GinPageGetOpaque(page)->maxoff;
	MemSet(&prev_iptr, 0, sizeof(ItemPointerData));
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		iptr = GinDataPageGetItemPointer(page, i);
		size += ginDataPageLeafGetItemPointerSize(iptr, &prev_iptr);
		prev_iptr = *iptr;
	}

	/*
	 * We should be able to hold at least one more TID for page compression be
	 * properly xlogged.
	 */
	if (size > GinDataLeafMaxPostingListSize - MAX_COMPRESSED_ITEM_POINTER_SIZE)
		return false;

	/* Make copy of page */
	memcpy(pageCopy, page, BLCKSZ);

	/* Reinitialize page as compressed */
	GinInitDataLeafPage(page);
	GinPageGetOpaque(page)->rightlink = GinPageGetOpaque(pageCopy)->rightlink;
	*GinDataPageGetRightBound(page) = *GinDataPageGetRightBound(pageCopy);
	PageSetLSN(page, PageGetLSN(pageCopy));

	/* Compress TIDs of pageCopy into page */
	ptr = GinDataLeafPageGetPostingList(page);
	cur = ptr;
	MemSet(&prev_iptr, 0, sizeof(ItemPointerData));
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		iptr = GinDataPageGetItemPointer(pageCopy, i);
		cur = ginDataPageLeafWriteItemPointer(cur, iptr, &prev_iptr);
		prev_iptr = *iptr;
	}
	GinDataLeafPageSetPostingListSize(page, cur - ptr);
	Assert(GinDataPageFreeSpacePre(page, cur) >= 0);
	updateItemIndexes(page);

	return true;
}

/*
 * Places keys to leaf data page and fills WAL record.
 */
static bool
dataPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack, XLogRecData **prdata)
{
	ItemPointer newItems = &btree->items[btree->curitem];
	int			maxitems = btree->nitem - btree->curitem;
	Page		page = BufferGetPage(buf);
	char	   *endPtr;
	Pointer 	ptr, insertBegin, insertEnd;
	ItemPointerData iptr = {{0, 0}, 0}, nextIptr;
	Pointer		restPtr;
	int			newlen;
	int			nextlen;
	int			restlen;
	int			freespace;
	int			i;
	ItemPointerData	rbound;
	int			oldsize;
	int			newsize;
	bool		compress = false;

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[2];
	static ginxlogInsertDataLeaf data;

	Assert(GinPageIsData(page));

	/* Try to compress page if it's uncompressed */
	if (!GinPageIsCompressed(page))
	{
		if (!dataCompressLeafPage(page))
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("can't compress block %u of gin index \"%s\", consider REINDEX",
					BufferGetBlockNumber(buf),
				   RelationGetRelationName(btree->ginstate->index))));
		}
		findInLeafPageCompressed(btree, page, &stack->iptr, &ptr);
		stack->off = InvalidOffsetNumber;
		stack->pageOffset = ptr - page;
		compress = true;
	}

	endPtr = GinDataLeafPageGetPostingListEnd(page);

	/*
	 * stack->pageOffset points to the location where we're going to insert
	 * the new item(s), and stack->iptr is the corresponding item pointer
	 * at that location.
	 */
	oldsize = GinDataLeafPageGetPostingListSize(page);
	insertBegin = page + stack->pageOffset;

	/*
	 * Read the next item pointer. This will become the first item pointer
	 * after the ones we insert.
	 */
	restPtr = insertBegin;
	if (insertBegin < endPtr)
	{
		nextIptr = stack->iptr;
		restPtr = ginDataPageLeafReadItemPointer(insertBegin, &nextIptr);
		restlen = GinDataLeafPageGetPostingListEnd(page) - restPtr;
		Assert(restlen >= 0);
		nextlen = restPtr - insertBegin;
		rbound = nextIptr;
	}
	else if (GinPageRightMost(page))
	{
		ItemPointerSetMax(&rbound);
		restlen = 0;
		nextlen = 0;
	}
	else
	{
		rbound = *GinDataPageGetRightBound(page);
		/*
		 * The right bound stored on page is inclusive, but for the comparison
		 * below, we want an exclusive value
		 */
		rbound.ip_posid++;
		restlen = 0;
		nextlen = 0;
	}

	/*
	 * Calculate how many of the new items we insert on this location, and how
	 * much space they take.
	 *
	 * Note: The size of the first item after the inserted one might change
	 * when it's re-encoded. But it can never become larger, because the
	 * distance from the previous item gets smaller, not larger. We ignore
	 * that effect, and possibly waste a few bytes.
	 */
	iptr = stack->iptr;
	newlen = 0;
	freespace = GinDataLeafPageGetFreeSpace(page);
	for (i = 0; i < maxitems; i++)
	{
		int		l;

		if (ginCompareItemPointers(&newItems[i], &rbound) >= 0)
		{
			/*
			 * This needs to go to some other location in the tree. (The
			 * caller should've chosen the insert location so that at least
			 * the first item goes here.)
			 */
			Assert(i > 0);
			break;
		}

		l = ginDataPageLeafGetItemPointerSize(&newItems[i], &iptr);
		if (l + newlen + nextlen > freespace)
		{
			/* this doesn't fit anymore */
			break;
		}

		newlen += l;
		iptr = newItems[i];
	}
	maxitems = i;
	if (maxitems == 0)
	{
		/*
		 * Page compression reserves space for at least one more TID. So it
		 * must be inserted.
		 */
		Assert(!compress);

		return false; /* no entries fit on this page */
	}

	/*
	 * We have to re-encode the first old item pointer after the ones we're
	 * inserting. Calculate how much space it will take re-encoded.
	 */
	if (insertBegin < endPtr)
	{
		ItemPointerData lastnew = newItems[maxitems - 1];

		nextlen = ginDataPageLeafGetItemPointerSize(&nextIptr, &lastnew);
	}

	/*
	 * Ok, we have collected all the information we need. Shift any entries
	 * after the insertion point that we are not going to otherwise modify.
	 */
	Assert(GinDataPageFreeSpacePre(page, restPtr + newlen + nextlen + restlen) >= 0);
	insertEnd = insertBegin + newlen + nextlen;
	if (restlen > 0)
		memmove(insertEnd, restPtr, restlen);

	/* Write the new items */
	ptr = insertBegin;
	iptr = stack->iptr;
	for (i = 0; i < maxitems; i++)
	{
		ptr = ginDataPageLeafWriteItemPointer(ptr, &newItems[i], &iptr);
		Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);
		iptr = newItems[i];
	}

	/* Write the re-encoded old item */
	if (nextlen > 0)
	{
		ptr = ginDataPageLeafWriteItemPointer(ptr, &nextIptr, &iptr);
		Assert(GinDataPageFreeSpacePre(page,ptr) >= 0);
	}
	Assert(ptr == insertEnd);

	newsize = insertEnd + restlen - GinDataLeafPageGetPostingList(page);
	GinDataLeafPageSetPostingListSize(page, newsize);

	/*---
	 * Update indexes in the end of page. How do we update item indexes is
	 * illustrated in following picture. Let item indexes 1 and 2 are item
	 * pointers just before and after next TID after place we inserting new
	 * TIDs.
	 *
	 *                  1          2
	 * +----------------+----------+-----------+
	 * | untouched TIDs | next TID | rest TIDs |
	 * +----------------+----------+-----------+
	 *                  ^
	 *                  |
	 *                  Place where we insert new TIDs
	 *
	 *                  1                     2
	 * +----------------+----------+----------+-----------+
	 * | untouched TIDs | new TIDs | next TID | rest TIDs |
	 * +----------------+----------+----------+-----------+
	 *
	 * Item index 1 wasn't updated because previous TID remaining the same for
	 * this offset. Item index 2 was shifted because of both inserting new TIDs
	 * and possible change of next TID size. Same shift apply to the all
	 * subsequent item indexes.
	 */
	incrUpdateItemIndexes(page, restPtr - GinDataLeafPageGetPostingList(page), newsize - oldsize);

	/* Put WAL data */
	*prdata = rdata;

	data.common.node = btree->index->rd_node;
	data.common.blkno = BufferGetBlockNumber(buf);
	data.common.updateBlkno = InvalidBlockNumber;
	data.common.isData = TRUE;
	data.common.isLeaf = TRUE;

	data.beginOffset = insertBegin - GinDataLeafPageGetPostingList(page);
	data.newlen = insertEnd - insertBegin;
	data.restOffset = restPtr - GinDataLeafPageGetPostingList(page);

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = offsetof(ginxlogInsertDataLeaf, newdata);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = buf;
	rdata[1].buffer_std = TRUE;
	rdata[1].data = insertBegin;
	rdata[1].len = insertEnd - insertBegin;
	rdata[1].next = NULL;

	btree->curitem += maxitems;

	Assert(GinDataLeafPageGetFreeSpace(page) <= freespace);

	return true;
}

/*
 * Places keys to internal data page and fills WAL record.
 */
static bool
dataPlaceToPageInternal(GinBtree btree, Buffer buf, GinBtreeStack *stack,
						XLogRecData **prdata)
{
	Page		page = BufferGetPage(buf);
	int			cnt = 0;
	OffsetNumber off = stack->off;

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[2];
	static ginxlogInsertDataInternal data;

	/* quick exit if it doesn't fit */
	if (GinNonLeafDataPageGetFreeSpace(page) < sizeof(PostingItem))
		return false;

	*prdata = rdata;

	data.common.updateBlkno = dataPrepareData(btree, page, off);
	data.common.node = btree->index->rd_node;
	data.common.blkno = BufferGetBlockNumber(buf);
	data.common.isData = TRUE;
	data.common.isLeaf = FALSE;
	data.offset = off;
	data.newitem = btree->pitem;

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
		rdata[0].buffer_std = FALSE;
		rdata[0].data = NULL;
		rdata[0].len = 0;
		rdata[0].next = &rdata[1];
		cnt++;
	}

	rdata[cnt].buffer = InvalidBuffer;
	rdata[cnt].data = (char *) &data;
	rdata[cnt].len = sizeof(ginxlogInsertDataInternal);
	rdata[cnt].next = NULL;

	GinDataPageAddPostingItem(page, &(btree->pitem), off);

	return true;
}

/*
 * Place tuple and split page, original buffer(lbuf) leaves untouched,
 * returns shadow page of lbuf filled new data.
 */
static Page
dataSplitPageLeaf(GinBtree btree, Buffer lbuf, Buffer rbuf, OffsetNumber off,
				  XLogRecData **prdata)
{
	Size		totalsize;
	Pointer		ptr;
	Pointer		oldPtr, oldEndPtr;
	Page		oldPage = BufferGetPage(lbuf);
	Page		page;
	Page		lpage = PageGetTempPage(oldPage);
	Page		rpage = BufferGetPage(rbuf);
	ItemPointerData iptr, prevIptr, maxLeftIptr;
	ItemPointerData nextold, nextnew;
	int			maxItemIndex = btree->curitem;

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[3];
	static ginxlogSplit data;

	*prdata = rdata;
	data.leftChildBlkno = InvalidOffsetNumber;
	data.updateBlkno = InvalidBlockNumber;

	/*
	 * Estimate the total size of the posting lists we're going to store on
	 * the two halves.
	 *
	 * Try to place as many items as we can if we're placing them to the end
	 * of rightmost page. Otherwise place one item. We'll reserve two
	 * MAX_COMPRESSED_ITEM_POINTER_SIZE. One because possible unevenness of
	 * split and another because of re-encoding first item pointer of right
	 * page from zero.
	 */
	MemSet(&iptr, 0, sizeof(ItemPointerData));
	totalsize = GinDataLeafPageGetPostingListSize(oldPage);
	if (off == InvalidOffsetNumber && GinPageRightMost(oldPage))
	{
		while (maxItemIndex < btree->nitem && totalsize +
			ginDataPageLeafGetItemPointerSize(&btree->items[maxItemIndex], &iptr)
			< 2 * GinDataLeafMaxPostingListSize - 2 * MAX_COMPRESSED_ITEM_POINTER_SIZE)
		{
			totalsize += ginDataPageLeafGetItemPointerSize(
											&btree->items[maxItemIndex], &iptr);
			iptr = btree->items[maxItemIndex];
			maxItemIndex++;
		}
		Assert(maxItemIndex > btree->curitem);
	}
	else
	{
		totalsize += ginDataPageLeafGetItemPointerSize(
											&btree->items[maxItemIndex], &iptr);
		Assert(totalsize <
					2 * GinDataLeafMaxPostingListSize - 2 * MAX_COMPRESSED_ITEM_POINTER_SIZE);
	}

	/* Reinitialize pages */
	GinInitDataLeafPage(lpage);
	GinInitDataLeafPage(rpage);
	GinDataLeafPageSetPostingListSize(lpage, 0);
	GinDataLeafPageSetPostingListSize(rpage, 0);

	/*
	 * Copy the old and new items to the new pages.
	 */
	MemSet(&nextold, 0, sizeof(ItemPointerData));
	oldPtr = GinDataLeafPageGetPostingList(oldPage);
	oldEndPtr = GinDataLeafPageGetPostingListEnd(oldPage);
	if (oldPtr < oldEndPtr)
		oldPtr = ginDataPageLeafReadItemPointer(oldPtr, &nextold);
	else
		ItemPointerSetInvalid(&nextold);

	nextnew = btree->items[btree->curitem];

	MemSet(&prevIptr, 0, sizeof(ItemPointerData));
	ptr = GinDataLeafPageGetPostingList(lpage);
	page = lpage;
	while (ItemPointerIsValid(&nextnew) || ItemPointerIsValid(&nextold))
	{
		ItemPointerData item;
		int			cmp;

		/*
		 * Pull the next tuple from the old page, or from the array of new
		 * items to place.
		 */
		if (!ItemPointerIsValid(&nextnew))
			cmp = -1;
		else if (!ItemPointerIsValid(&nextold))
			cmp = 1;
		else
			cmp = ginCompareItemPointers(&nextold, &nextnew);

		Assert(cmp != 0); /* duplicates are not expected */
		if (cmp < 0)
		{
			item = nextold;
			if (oldPtr < oldEndPtr)
				oldPtr = ginDataPageLeafReadItemPointer(oldPtr, &nextold);
			else
				ItemPointerSetInvalid(&nextold);
		}
		else
		{
			item = nextnew;
			btree->curitem++;
			if (btree->curitem < maxItemIndex)
				nextnew = btree->items[btree->curitem];
			else
				ItemPointerSetInvalid(&nextnew);
		}

		/* Write the tuple to the current half. */
		ptr = ginDataPageLeafWriteItemPointer(ptr, &item, &prevIptr);
		Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);
		prevIptr = item;

		/* Check if it's time to switch to the right page */
		if (ptr - GinDataLeafPageGetPostingList(page) > totalsize / 2 &&
			page == lpage)
		{
			maxLeftIptr = item;
			GinDataLeafPageSetPostingListSize(lpage, ptr - GinDataLeafPageGetPostingList(lpage));
			MemSet(&prevIptr, 0, sizeof(ItemPointerData));
			page = rpage;
			ptr = GinDataLeafPageGetPostingList(rpage);
		}
	}

	Assert(page == rpage);
	GinDataLeafPageSetPostingListSize(rpage, ptr - GinDataLeafPageGetPostingList(rpage));

	*GinDataPageGetRightBound(rpage) = *GinDataPageGetRightBound(oldPage);
	*GinDataPageGetRightBound(lpage) = maxLeftIptr;

	Assert(GinPageRightMost(oldPage) ||
			ginCompareItemPointers(GinDataPageGetRightBound(lpage),
			GinDataPageGetRightBound(rpage)) < 0);

	/* Fill indexes at the end of pages */
	updateItemIndexes(lpage);
	updateItemIndexes(rpage);

	data.node = btree->index->rd_node;
	data.rootBlkno = InvalidBlockNumber;
	data.lblkno = BufferGetBlockNumber(lbuf);
	data.rblkno = BufferGetBlockNumber(rbuf);
	data.separator = GinDataLeafPageGetPostingListSize(lpage);
	data.nitem = GinDataLeafPageGetPostingListSize(lpage) + GinDataLeafPageGetPostingListSize(rpage);
	data.isData = TRUE;
	data.isLeaf = TRUE;
	data.isRootSplit = FALSE;
	data.rightbound = *GinDataPageGetRightBound(rpage);

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(ginxlogSplit);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = GinDataLeafPageGetPostingList(lpage);
	rdata[1].len = GinDataLeafPageGetPostingListSize(lpage);
	rdata[1].next = &rdata[2];

	rdata[2].buffer = InvalidBuffer;
	rdata[2].data = GinDataLeafPageGetPostingList(rpage);
	rdata[2].len = GinDataLeafPageGetPostingListSize(rpage);
	rdata[2].next = NULL;

	/* Prepare a downlink tuple for insertion to the parent */
	PostingItemSetBlockNumber(&(btree->pitem), BufferGetBlockNumber(lbuf));
	btree->pitem.key = maxLeftIptr;
	btree->rightblkno = BufferGetBlockNumber(rbuf);

	return lpage;
}

/*
 * Split page and fill WAL record. Returns a new temp buffer filled with data
 * that should go to the left page. The original buffer (lbuf) is left
 * untouched.
 */
static Page
dataSplitPageInternal(GinBtree btree, Buffer lbuf, Buffer rbuf,
					  OffsetNumber off, XLogRecData **prdata)
{
	Page		oldpage = BufferGetPage(lbuf);
	int			nitems = GinPageGetOpaque(oldpage)->maxoff;
	Size		pageSize = PageGetPageSize(oldpage);
	ItemPointerData oldbound = *GinDataPageGetRightBound(oldpage);
	ItemPointer	bound;
	Page		lpage;
	Page		rpage;
	OffsetNumber separator;

	/* these must be static so they can be returned to caller */
	static ginxlogSplit data;
	static XLogRecData rdata[4];
	static PostingItem allitems[(BLCKSZ / sizeof(PostingItem)) + 1];

	lpage = PageGetTempPage(oldpage);
	GinInitPage(lpage, GinPageGetOpaque(lpage)->flags, pageSize);
	rpage = BufferGetPage(rbuf);
	GinInitPage(rpage, GinPageGetOpaque(rpage)->flags, pageSize);

	*prdata = rdata;
	data.leftChildBlkno = PostingItemGetBlockNumber(&(btree->pitem));
	data.updateBlkno = dataPrepareData(btree, oldpage, off);

	/*
	 * First construct a new list of PostingItems, which includes all the
	 * old items, and the new item.
	 */
	memcpy(allitems, GinDataPageGetPostingItem(oldpage, FirstOffsetNumber),
		   (off - 1) * sizeof(PostingItem));
	allitems[off - 1] = btree->pitem;
	memcpy(&allitems[off], GinDataPageGetPostingItem(oldpage, off),
		   (nitems - (off - 1)) * sizeof(PostingItem));
	nitems++;

	/*
	 * When creating a new index, fit as many tuples as possible on the left
	 * page, on the assumption that the table is scanned from beginning to
	 * end. This packs the index as tight as possible.
	 */
	if (btree->isBuild && GinPageRightMost(oldpage))
		separator = GinNonLeafDataPageGetFreeSpace(rpage) / sizeof(PostingItem);
	else
		separator = nitems / 2;

	memcpy(GinDataPageGetPostingItem(lpage, FirstOffsetNumber), allitems, separator * sizeof(PostingItem));
	GinPageGetOpaque(lpage)->maxoff = separator;
	memcpy(GinDataPageGetPostingItem(rpage, FirstOffsetNumber),
		 &allitems[separator], (nitems - separator) * sizeof(PostingItem));
	GinPageGetOpaque(rpage)->maxoff = nitems - separator;

	/* set up right bound for left page */
	bound = GinDataPageGetRightBound(lpage);
	*bound = GinDataPageGetPostingItem(lpage,
								  GinPageGetOpaque(lpage)->maxoff)->key;

	/* set up right bound for right page */
	*GinDataPageGetRightBound(rpage) = oldbound;

	data.node = btree->index->rd_node;
	data.rootBlkno = InvalidBlockNumber;
	data.lblkno = BufferGetBlockNumber(lbuf);
	data.rblkno = BufferGetBlockNumber(rbuf);
	data.separator = separator;
	data.nitem = nitems;
	data.isData = TRUE;
	data.isLeaf = FALSE;
	data.isRootSplit = FALSE;
	data.rightbound = oldbound;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(ginxlogSplit);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = (char *) allitems;
	rdata[1].len = nitems * sizeof(PostingItem);
	rdata[1].next = NULL;

	/* Prepare a downlink tuple for insertion to the parent */
	PostingItemSetBlockNumber(&(btree->pitem), BufferGetBlockNumber(lbuf));
	btree->pitem.key = *GinDataPageGetRightBound(lpage);
	btree->rightblkno = BufferGetBlockNumber(rbuf);

	return lpage;
}

/*
 * Split page of posting tree. Calls relevant function of internal of leaf page
 * because they are handled very differently.
 */
static Page
dataSplitPage(GinBtree btree, Buffer lbuf, Buffer rbuf, OffsetNumber off,
			  XLogRecData **prdata)
{
	if (GinPageIsLeaf(BufferGetPage(lbuf)))
		return dataSplitPageLeaf(btree, lbuf, rbuf, off, prdata);
	else
		return dataSplitPageInternal(btree, lbuf, rbuf, off, prdata);
}


/*
 * Places an item (or items) to a posting tree. Calls relevant function of
 * internal of leaf page because they are handled very differently.
 */
static bool
dataPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack, XLogRecData **prdata)
{
	Assert(GinPageIsData(BufferGetPage(buf)));

	if (GinPageIsLeaf(BufferGetPage(buf)))
		return dataPlaceToPageLeaf(btree, buf, stack, prdata);
	else
		return dataPlaceToPageInternal(btree, buf, stack, prdata);
}

/*
 * Updates indexes in the end of leaf page which are used for faster search.
 * Also updates freespace opaque field of page. Returns last item pointer of
 * page.
 */
ItemPointerData
updateItemIndexes(Page page)
{
	GinDataLeafItemIndex *indexes = GinPageGetIndexes(page);
	Pointer		beginptr;
	Pointer		ptr;
	Pointer		endptr;
	Pointer		nextptr;
	ItemPointerData iptr;
	int			i, j;

	Assert(GinPageIsLeaf(page));

	/* Iterate over page */

	ptr = beginptr = GinDataLeafPageGetPostingList(page);
	endptr = GinDataLeafPageGetPostingListEnd(page);
	MemSet(&iptr, 0, sizeof(ItemPointerData));

	nextptr = beginptr + (int) ((double) GinDataLeafMaxPostingListSize / (double) (GinDataLeafIndexCount + 1));
	j = 0;
	i = FirstOffsetNumber;
	while (ptr < endptr && j < GinDataLeafIndexCount)
	{
		/* Place next page index entry if it's time to */
		if (ptr >= nextptr)
		{
			indexes[j].iptr = iptr;
			indexes[j].pageOffset = ptr - GinDataLeafPageGetPostingList(page);
			j++;
			nextptr = beginptr + (int) ((double) (j + 1) * (double) GinDataLeafMaxPostingListSize / (double) (GinDataLeafIndexCount + 1));
		}
		ptr = ginDataPageLeafReadItemPointer(ptr, &iptr);
		i++;
	}

	/* Fill rest of page indexes with InvalidOffsetNumber */
	for (; j < GinDataLeafIndexCount; j++)
	{
		MemSet(&indexes[j].iptr, 0, sizeof(ItemPointerData));
		indexes[j].pageOffset = 0;
	}

	return iptr;
}

/*
 * Fills new root by right bound values from child.
 * Also called from ginxlog, should not use btree
 */
void
ginDataFillRoot(GinBtree btree, Buffer root, Buffer lbuf, Buffer rbuf)
{
	Page		page = BufferGetPage(root),
				lpage = BufferGetPage(lbuf),
				rpage = BufferGetPage(rbuf);
	PostingItem li,
				ri;

	li.key = *GinDataPageGetRightBound(lpage);
	PostingItemSetBlockNumber(&li, BufferGetBlockNumber(lbuf));
	GinDataPageAddPostingItem(page, &li, InvalidOffsetNumber);

	ri.key = *GinDataPageGetRightBound(rpage);
	PostingItemSetBlockNumber(&ri, BufferGetBlockNumber(rbuf));
	GinDataPageAddPostingItem(page, &ri, InvalidOffsetNumber);
}

/*
 * Creates new posting tree containing the given TIDs. Returns the page
 * number of the root of the new posting tree.
 *
 * items[] must be in sorted order with no duplicates.
 */
BlockNumber
createPostingTree(Relation index, ItemPointerData *items, uint32 nitems,
				  GinStatsData *buildStats)
{
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;
	int			i;
	CompressedPostingList ptr,
				cur;
	ItemPointerData prev_iptr = {{0,0},0};
	Size		size;
	int			nrootitems;

	/*
	 * Calculate how many TIDs will fit on the first page
	 */
	MemSet(&prev_iptr, 0, sizeof(ItemPointerData));
	size = 0;
	nrootitems = 0;
	while (nrootitems < nitems)
	{
		size += ginDataPageLeafGetItemPointerSize(&items[nrootitems], &prev_iptr);
		if (size > GinDataLeafMaxPostingListSize)
			break;
		prev_iptr = items[nrootitems];
		nrootitems++;
	}

	/*
	 * Create the root page.
	 */
	buffer = GinNewBuffer(index);
	page = BufferGetPage(buffer);
	blkno = BufferGetBlockNumber(buffer);

	START_CRIT_SECTION();

	GinInitDataLeafPage(page);

	ptr = GinDataLeafPageGetPostingList(page);
	cur = ptr;
	MemSet(&prev_iptr, 0, sizeof(ItemPointerData));
	for (i = 0; i < nrootitems; i++)
	{
		cur = ginDataPageLeafWriteItemPointer(cur, &items[i], &prev_iptr);
		prev_iptr = items[i];
	}
	GinDataLeafPageSetPostingListSize(page, cur - ptr);
	Assert(GinDataPageFreeSpacePre(page, cur) >= 0);
	updateItemIndexes(page);

	MarkBufferDirty(buffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		ginxlogCreatePostingTree data;
		static char		buf[BLCKSZ];

		data.node = index->rd_node;
		data.blkno = blkno;
		data.size = cur - ptr;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &data;
		rdata[0].len = sizeof(ginxlogCreatePostingTree);
		rdata[0].next = &rdata[1];

		memcpy(buf, GinDataLeafPageGetPostingList(page), GinDataLeafPageGetPostingListSize(page));

		rdata[1].buffer = InvalidBuffer;
		rdata[1].data = buf;
		rdata[1].len = GinDataLeafPageGetPostingListSize(page);
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_CREATE_PTREE, rdata);
		PageSetLSN(page, recptr);
	}

	UnlockReleaseBuffer(buffer);

	END_CRIT_SECTION();

	/* During index build, count the newly-added data page */
	if (buildStats)
		buildStats->nDataPages++;

	/*
	 * Add any remaining TIDs to the newly-created posting tree.
	 */
	if (nitems > nrootitems)
	{
		GinPostingTreeScan *gdi;

		gdi = ginPrepareScanPostingTree(index, blkno, FALSE);
		gdi->btree.isBuild = (buildStats != NULL);

		ginInsertItemPointers(gdi,
							  items + nrootitems,
							  nitems - nrootitems,
							  buildStats);

		pfree(gdi);
	}

	return blkno;
}

void
ginPrepareDataScan(GinBtree btree, Relation index)
{
	memset(btree, 0, sizeof(GinBtreeData));

	btree->index = index;

	btree->findChildPage = dataLocateItem;
	btree->isMoveRight = dataIsMoveRight;
	btree->findItem = dataLocateLeafItem;
	btree->findChildPtr = dataFindChildPtr;
	btree->getLeftMostPage = dataGetLeftMostPage;
	btree->placeToPage = dataPlaceToPage;
	btree->splitPage = dataSplitPage;
	btree->fillRoot = ginDataFillRoot;

	btree->isData = TRUE;
	btree->searchMode = FALSE;
	btree->isDelete = FALSE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;
}

GinPostingTreeScan *
ginPrepareScanPostingTree(Relation index, BlockNumber rootBlkno, bool searchMode)
{
	GinPostingTreeScan *gdi = (GinPostingTreeScan *) palloc0(sizeof(GinPostingTreeScan));

	ginPrepareDataScan(&gdi->btree, index);

	gdi->btree.searchMode = searchMode;
	gdi->btree.fullScan = searchMode;

	gdi->stack = ginPrepareFindLeafPage(&gdi->btree, rootBlkno);

	return gdi;
}

/*
 * Inserts array of item pointers, may execute several tree scan (very rare)
 */
void
ginInsertItemPointers(GinPostingTreeScan *gdi,
					  ItemPointerData *items, uint32 nitem,
					  GinStatsData *buildStats)
{
	BlockNumber rootBlkno = gdi->stack->blkno;

	gdi->btree.items = items;
	gdi->btree.nitem = nitem;
	gdi->btree.curitem = 0;

	while (gdi->btree.curitem < gdi->btree.nitem)
	{
		if (!gdi->stack)
			gdi->stack = ginPrepareFindLeafPage(&gdi->btree, rootBlkno);

		gdi->stack = ginFindLeafPage(&gdi->btree, gdi->stack);

		if (gdi->btree.findItem(&(gdi->btree), gdi->stack))
		{
			/*
			 * gdi->btree.items[gdi->btree.curitem] already exists in index
			 */
			gdi->btree.curitem++;
			LockBuffer(gdi->stack->buffer, GIN_UNLOCK);
			freeGinBtreeStack(gdi->stack);
		}
		else
			ginInsertValue(&(gdi->btree), gdi->stack, buildStats);

		gdi->stack = NULL;
	}
}

Buffer
ginScanBeginPostingTree(GinPostingTreeScan *gdi)
{
	gdi->stack = ginFindLeafPage(&gdi->btree, gdi->stack);
	return gdi->stack->buffer;
}
