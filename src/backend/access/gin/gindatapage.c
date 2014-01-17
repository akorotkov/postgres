/*-------------------------------------------------------------------------
 *
 * gindatapage.c
 *	  routines for handling GIN posting tree pages.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/gindatapage.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "access/heapam_xlog.h"
#include "miscadmin.h"
#include "utils/rel.h"

#define GinPostingListSegmentMaxSize (GinDataLeafMaxPostingListSize/32)
#define GinPostingListSegmentsMaxCount (64)
#define GinPostingListUnpackedMaxSize (GinDataLeafMaxPostingListSize/32)
#define GinPostingListMaxAppendItems (100)

static void GinInitDataLeafPage(Page page);
static int bsearch_itemptr(ItemPointer array, int nitems, ItemPointer key, bool *found);

static void dataSplitPageInternal(GinBtree btree, Buffer origbuf,
					  GinBtreeStack *stack,
					  void *insertdata, BlockNumber updateblkno,
					  XLogRecData **prdata, Page *newlpage, Page *newrpage);

/*
 * Get all TIDs from leaf data page to single uncompressed array.
 */
ItemPointer
GinDataLeafPageGetItems(Page page, int *nitems)
{
	ItemPointer all;
	int			nall;
	ItemPointer compressed;
	int			ncompressed;
	ItemPointer uncompressed;
	int			nuncompressed;

	if (GinPageIsCompressed(page))
	{
		/*
		 * Decode all the entries on the page.
		 */
		PostingListSegment *ptr = GinDataLeafPageGetPostingList(page);
		Size		len = GinDataLeafPageGetPostingListSize(page);

		compressed = ginPostingListDecodeAllSegments(ptr, len, &ncompressed);
	}
	else
	{
		compressed = NULL;
		ncompressed = 0;
	}

	uncompressed = GinDataLeafPageGetUncompressed(page, &nuncompressed);

	nall = nuncompressed + ncompressed;
	if (nuncompressed > 0 && ncompressed > 0)
	{
		/* merge the compressed and uncompressed items into one list. */
		all = (ItemPointer)palloc(nall * sizeof(ItemPointerData));
		nall = ginMergeItemPointers(all,
									compressed, ncompressed,
									uncompressed, nuncompressed);
		pfree(compressed);
	}
	else if (nuncompressed > 0)
	{
		all = (ItemPointer)palloc(nuncompressed * sizeof(ItemPointerData));
		memcpy(all, uncompressed, nuncompressed * sizeof(ItemPointerData));
	}
	else if (ncompressed > 0)
	{
		all = compressed;
	}
	else
		all = palloc(0);

	*nitems = nall;
	return all;
}

/*
 * Places all TIDs from leaf data page to bitmap.
 */
int
GinDataLeafPageGetItemsToTbm(Page page, TIDBitmap *tbm)
{
	ItemPointer uncompressed;
	int			nuncompressed;
	int			nall = 0;

	if (GinPageIsCompressed(page))
	{
		/*
		 * Decode all the entries on the page.
		 */
		PostingListSegment *segment = GinDataLeafPageGetPostingList(page);
		Size		len = GinDataLeafPageGetPostingListSize(page);

		nall += ginPostingListDecodeToTbm(segment, len, tbm);
	}

	uncompressed = GinDataLeafPageGetUncompressed(page, &nuncompressed);
	nall += nuncompressed;

	if (nuncompressed > 0)
		tbm_add_tuples(tbm, uncompressed, nuncompressed, false);

	return nall;
}

/*
 * Initialize leaf page of posting tree. Reserves space for item indexes at
 * the end of page.
 */
static void
GinInitDataLeafPage(Page page)
{
	GinInitPage(page, GIN_DATA | GIN_LEAF | GIN_COMPRESSED, BLCKSZ);
}

/*
 * Get a pointer to the uncompressed part of page.
 */
ItemPointer
GinDataLeafPageGetUncompressed(Page page, int *nitems)
{
	ItemPointer items;

	if (GinPageIsCompressed(page))
	{
		PageHeader phdr = (PageHeader) page;
		items = (ItemPointer) (page + phdr->pd_upper);

		*nitems = (phdr ->pd_special - phdr->pd_upper) / sizeof(ItemPointerData);
	}
	else
	{
		items = GinDataPageGetItemPointer(page, FirstOffsetNumber);
		*nitems = GinPageGetOpaque(page)->maxoff;
	}

#ifdef USE_ASSERT_CHECKING
	if (assert_enabled)
	{
		/* Check if TIDs are really ascending */
		int i;

		for (i = 1; i < *nitems; i++)
		{
			Assert(ginCompareItemPointers(&items[i - 1], &items[i]) < 0);
		}
	}
#endif

	return items;
}

/*
 * Check if we should follow the right link to find the item we're searching
 * for.
 *
 * Compares inserting item pointer with the right bound of the current page.
 */
static bool
dataIsMoveRight(GinBtree btree, Page page)
{
	ItemPointer iptr = GinDataPageGetRightBound(page);

	if (GinPageRightMost(page))
		return FALSE;

	return (ginCompareItemPointers(&btree->itemptr, iptr) > 0) ? TRUE : FALSE;
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
		return btree->getLeftMostChild(btree, page);
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
			result = ginCompareItemPointers(&btree->itemptr, &(pitem->key));
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
findInLeafPageCompressed(GinBtree btree, Page page)
{
	PostingListSegment *segment;
	Pointer		endPtr;
	bool		result = false;
	ItemPointer	items;
	int			nitems;

	/*
	 * Walk the segments until we find the one containing the item we're
	 * searching for.
	 */
	segment = GinDataLeafPageGetPostingList(page);
	endPtr =  (char *) segment + GinDataLeafPageGetPostingListSize(page);
	while (ginCompareItemPointers(&segment->first, &btree->itemptr) < 0)
	{
		PostingListSegment *nextsegment = NextPostingListSegment(segment);
		if ((char *) nextsegment >= endPtr)
		{
			/* this was the last segment */
			Assert ((char *) nextsegment == endPtr);
			break;
		}
		segment = (PostingListSegment *) nextsegment;
	}
	Assert((char *) segment <= endPtr);

	/* Scan the segment */
	items = ginPostingListDecodeSegment(segment, &nitems);
	(void) bsearch_itemptr(items, nitems, &btree->itemptr, &result);
	pfree(items);

	return result;
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
	ItemPointer		items;
	int				nitems;

	Assert(GinPageIsData(page) && GinPageIsLeaf(page));

	/* Check if the item is in the uncompressed part of the page */
	items = GinDataLeafPageGetUncompressed(page, &nitems);

	if (nitems > 0)
	{
		bsearch_itemptr(items, nitems, &btree->itemptr, &result);
		if (result)
			return true;
	}

	/* Search the compressed parts */
	if (GinPageIsCompressed(page))
		result = findInLeafPageCompressed(btree, page);

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

	Assert(PostingItemGetBlockNumber(data) != InvalidBlockNumber);
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
 * Binary search of an array of item pointers.
 */
static int
bsearch_itemptr(ItemPointer array, int nitems, ItemPointer key, bool *found)
{
	OffsetNumber low,
				high;
	int			result;

	low = 0;
	high = nitems;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		result = ginCompareItemPointers(key, &array[mid]);
		if (result == 0)
		{
			*found = true;
			return mid;
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	Assert(high <= nitems);
	*found = false;
	return high;
}

/*
 * Places keys to leaf data page and fills WAL record.
 */
static bool
dataPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack,
					void *insertdata, XLogRecData **prdata,
					Page *newlpage, Page *newrpage)
{
	GinBtreeDataLeafInsertData *items = insertdata;
	ItemPointer newItems = &items->items[items->curitem];
	int			maxitems = items->nitem - items->curitem;
	Page		page = BufferGetPage(buf);
	Page		oldPage = page;
	int			i;
	ItemPointerData	rbound;
	ItemPointer olditems;
	int			nolditems;
	ItemPointer allitems;
	int			nallitems;
	Page		lpage;
	Page		rpage;
	ListCell   *lc;
	bool		wassplit;
	ItemPointerData	maxiptrs[GinPostingListSegmentsMaxCount];
	char	   *ptr;
	bool		append;
	int			totalpacked;
	Size		lsize;
	PostingListSegment *seg, *prevseg, *selseg, *prevselseg;
	List	   *lsegments = NIL;
	int			npacked;
	int			segsize;
	ItemPointer olduncompressed;
	int			nolduncompressed;
	int			untouchedsize;
	Size		freespace;
	Pointer		segend;
	ItemPointerData minNewItem;

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[3];
	static ginxlogInsertDataLeaf insert_xlog;
	static ginxlogSplitDataLeaf split_xlog;

	*prdata = rdata;

	Assert(GinPageIsData(page));

	/*
	 * Count how many of the new items belong to this page.
	 */
	if (!GinPageRightMost(page))
	{
		rbound  = *GinDataPageGetRightBound(page);

		for (i = 0; i < maxitems; i++)
		{
			if (ginCompareItemPointers(&newItems[i], &rbound) > 0)
			{
				/*
				 * This needs to go to some other location in the tree. (The
				 * caller should've chosen the insert location so that at least
				 * the first item goes here.)
				 */
				Assert(i > 0);
				break;
			}
		}
		maxitems = i;
	}

	olduncompressed = GinDataLeafPageGetUncompressed(page, &nolduncompressed);

	/*
	 * First, check if all the new items fit on the page uncompressed. Always
	 * re-encode the page if it was in the pre-9.4 old format.
	 */
	if (GinPageIsCompressed(page))
		freespace = GinDataLeafPageGetFreeSpace(page);
	else
		freespace = 0;
	freespace = Min(freespace, GinPostingListUnpackedMaxSize);

	if (freespace >= sizeof(ItemPointerData) * maxitems)
	{
		PageHeader phdr = (PageHeader) page;
		int			upper;
		int			totalitems;

		START_CRIT_SECTION();

		/* Merge in the new items */
		upper = phdr->pd_upper - sizeof(ItemPointerData) * maxitems;
		totalitems = ginMergeItemPointers((ItemPointer) (page + upper),
										  olduncompressed, nolduncompressed,
										  newItems, maxitems);

		if (totalitems < nolduncompressed + maxitems)
		{
			int			nduplicates = nolduncompressed + maxitems - totalitems;
			memmove(page + upper + sizeof(ItemPointerData) * nduplicates,
					page + upper,
					sizeof(ItemPointerData) * totalitems);
			upper += sizeof(ItemPointerData) * nduplicates;
			Assert(false);
		}
		phdr->pd_upper = upper;
		Assert(phdr->pd_upper >= phdr->pd_lower);

		/* Put WAL data */
		insert_xlog.length = sizeof(ItemPointerData) * maxitems;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &insert_xlog;
		rdata[0].len = offsetof(ginxlogInsertDataLeaf, newdata);
		rdata[0].next = &rdata[1];

		rdata[1].buffer = buf;
		rdata[1].buffer_std = TRUE;
		rdata[1].data = (char *) newItems;
		rdata[1].len = sizeof(ItemPointerData) * maxitems;
		rdata[1].next = NULL;

		*prdata = rdata;

		items->curitem += maxitems;

		elog(DEBUG2, "inserted %d items to block %u; fit uncompressed",
			 maxitems, BufferGetBlockNumber(buf));

		return true;
	}

	/*
	 * Didn't fit uncompressed. We'll have to encode them. Check if both
	 * new items and uncompressed items can be placed starting from last
	 * segment of page. Then re-encode only last segment of page.
	 */
	minNewItem = newItems[0];
	if (nolduncompressed == 0 &&
			ginCompareItemPointers(&olduncompressed[0], &minNewItem) < 0)
		minNewItem = olduncompressed[0];

	seg = GinDataLeafPageGetPostingList(page);
	segend = ((Pointer)seg) + GinDataLeafPageGetPostingListSize(page);
	prevseg = NULL;

	selseg = seg;
	prevselseg = prevseg;
	while ((Pointer)seg < segend)
	{
		if (ginCompareItemPointers(&minNewItem, &seg->first))
		{
			selseg = seg;
			prevselseg = prevseg;
		}
		prevseg = seg;
		seg = NextPostingListSegment(seg);
	}

	olditems = ginPostingListDecodeAllSegments(selseg,
			segend - (Pointer)selseg, &nolditems);
	untouchedsize = (Pointer)selseg - (Pointer)GinDataLeafPageGetPostingList(page);

	if (nolduncompressed > 0)
	{
		/*
		 * Merge any existing uncompressed items on the page with the
		 * compressed, decoded ones.
		 */
		ItemPointer tmp = palloc(sizeof(ItemPointerData) * (nolduncompressed + nolditems));

		nolditems = ginMergeItemPointers(tmp,
										 olditems, nolditems,
										 olduncompressed, nolduncompressed);
		pfree(olditems);
		olditems = tmp;
	}

	/*
	 * If we're appending to the end of the page, fit as many items as we can.
	 * Otherwise we have to limit the number of new items to insert, because
	 * we must make sure that all the old items still fit, so once we start
	 * packing, we can't just stop when we run out of space.
	 */
	if (nolditems == 0 || ginCompareItemPointers(&newItems[0], &olditems[nolditems - 1]) > 0)
	{
		append = true;
	}
	else
	{
		append = false;
		/*
		 * It seems safe to assume that we can fit GinPostingListMaxAppendItems
		 * new items after splitting.
		 */
		maxitems = Min(maxitems, GinPostingListMaxAppendItems);
	}

	if (nolditems > 0)
	{
		allitems = palloc((nolditems + maxitems) * sizeof(ItemPointerData));
		nallitems = ginMergeItemPointers(allitems,
										 olditems, nolditems,
										 newItems, maxitems);
		pfree(olditems);
	}
	else
	{
		allitems = newItems;
		nallitems = maxitems;
	}

	/*
	 * Start packing the items into segments.
	 */
	totalpacked = 0;
	lsize = untouchedsize;
	while (totalpacked < nallitems)
	{
		seg = ginCompressPostingList(&allitems[totalpacked],
										 nallitems - totalpacked,
										 GinPostingListSegmentMaxSize,
										 &npacked);
		segsize = SizeOfPostingListSegment(seg);
		if (lsize + segsize > GinDataLeafMaxPostingListSize)
			break;

		lsegments = lappend(lsegments, seg);
		lsize += segsize;
		totalpacked += npacked;
		maxiptrs[list_length(lsegments) - 1] = allitems[totalpacked - 1];
	}

	if (totalpacked == nallitems)
	{
		pfree(allitems);
		/* Great, all the items fit on a single page */

		/*
		 * Once we start modifying the page, there's no turning back. The caller
		 * is responsible for calling END_CRIT_SECTION() after writing the WAL
		 * record.
		 */
		START_CRIT_SECTION();

		ptr = (char *) GinDataLeafPageGetPostingList(page);
		ptr += untouchedsize;
		foreach(lc, lsegments)
		{
			seg = lfirst(lc);
			segsize = SizeOfPostingListSegment(seg);
			memcpy(ptr, seg, segsize);
			ptr += segsize;
			pfree(seg);
		}
		GinDataLeafPageSetPostingListSize(page, lsize);

		GinPageSetCompressed(page);
		((PageHeader) page)->pd_upper = ((PageHeader) page)->pd_special;

		/* Put WAL data */
		insert_xlog.length = INSERT_REENCODE_FLAG | (uint16) lsize;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &insert_xlog;
		rdata[0].len = offsetof(ginxlogInsertDataLeaf, newdata);
		rdata[0].next = &rdata[1];

		rdata[1].buffer = buf;
		rdata[1].buffer_std = TRUE;
		rdata[1].data = ((char *) GinDataLeafPageGetPostingList(page));
		rdata[1].len = lsize;
		rdata[1].next = NULL;

		*prdata = rdata;

		wassplit = false;

		elog(DEBUG2, "inserted %d items to block %u; re-encoded %d/%d to %d bytes",
			 maxitems, BufferGetBlockNumber(buf), 
			 nolduncompressed, nolditems,
			 (int) lsize);
	}
	else
	{
		int			totalsize;
		List	   *rsegments = NIL;
		Size		rsize;

		lpage = PageGetTempPage(BufferGetPage(buf));
		rpage = PageGetTempPage(BufferGetPage(buf));
		GinInitDataLeafPage(lpage);
		GinInitDataLeafPage(rpage);

		/*
		 * Have to split. Continue packing the items into segments, until the
		 * right page is full too, or we have packed everything.
		 */
		rsize = 0;
		while (totalpacked < nallitems)
		{
			seg = ginCompressPostingList(&allitems[totalpacked],
											 nallitems - totalpacked,
											 GinPostingListSegmentMaxSize,
											 &npacked);
			segsize = SizeOfPostingListSegment(seg);

			if (rsize + segsize > GinDataLeafMaxPostingListSize)
			{
				if (!append)
					elog(PANIC, "could not split GIN page, didn't fit");

				/*
				 * Adjust maxitems for the number of new items actually
				 * appended.
				 */
				maxitems -= nallitems - totalpacked;
				break;
			}

			rsegments = lappend(rsegments, seg);
			rsize += segsize;
			totalpacked += npacked;
		}
		pfree(allitems);
		totalsize = lsize + rsize;
		Assert(lsize <= GinDataLeafMaxPostingListSize);
		Assert(rsize <= GinDataLeafMaxPostingListSize);

		/*
		 * Ok, we've now packed into segments all the items we will insert,
		 * and divided the segments between the left and the right page. The
		 * left page was filled as full as possible, with the rest overflowed
		 * to the right page. When building a new index, that's good, because
		 * the table is table is scanned from beginning to end so there won't
		 * be any more insertions to the left page. This packs the index as
		 * tight as possible. But otherwise, split 50/50, by moving segments
		 * from the left page to the right page.
		 */
		if (!btree->isBuild)
		{
			while (lsegments)
			{
				seg = llast(lsegments);
				segsize = SizeOfPostingListSegment(seg);
				if (lsize - segsize < rsize + segsize)
					break;

				lsegments = list_delete_ptr(lsegments, seg);
				lsize -= segsize;
				rsegments = lcons(seg, rsegments);
				rsize += segsize;
			}
		}

		Assert(lsize <= GinDataLeafMaxPostingListSize);
		Assert(rsize <= GinDataLeafMaxPostingListSize);

		/* Ok, copy the segments to the pages */
		ptr = (char *) GinDataLeafPageGetPostingList(lpage);
		memcpy(ptr, GinDataLeafPageGetPostingList(page), untouchedsize);
		ptr += untouchedsize;
		foreach(lc, lsegments)
		{
			seg = lfirst(lc);
			segsize = SizeOfPostingListSegment(seg);

			memcpy(ptr, seg, segsize);
			ptr += segsize;
			pfree(seg);
		}
		GinDataLeafPageSetPostingListSize(lpage, lsize);

		if (lsegments == NIL)
		{
			ItemPointer	tmpitems;
			int			tmpcount;

			Assert(prevselseg);
			tmpitems = ginPostingListDecodeSegment(prevselseg, &tmpcount);
			*GinDataPageGetRightBound(lpage) = tmpitems[tmpcount - 1];
			pfree(tmpitems);
		}
		else
		{
			*GinDataPageGetRightBound(lpage) = maxiptrs[list_length(lsegments) - 1];
		}

		ptr = (char *) GinDataLeafPageGetPostingList(rpage);
		foreach(lc, rsegments)
		{
			seg = lfirst(lc);
			segsize = SizeOfPostingListSegment(seg);

			memcpy(ptr, seg, segsize);
			ptr += segsize;
			pfree(seg);
		}
		GinDataLeafPageSetPostingListSize(rpage, rsize);
		*GinDataPageGetRightBound(rpage) = *GinDataPageGetRightBound(oldPage);

		Assert(GinPageRightMost(oldPage) ||
			   ginCompareItemPointers(GinDataPageGetRightBound(lpage),
									  GinDataPageGetRightBound(rpage)) < 0);

		split_xlog.separator = lsize;
		split_xlog.nbytes = totalsize;
		split_xlog.lrightbound = *GinDataPageGetRightBound(lpage);
		split_xlog.rrightbound = *GinDataPageGetRightBound(rpage);

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &split_xlog;
		rdata[0].len = sizeof(ginxlogSplitDataLeaf);
		rdata[0].next = &rdata[1];

		rdata[1].buffer = InvalidBuffer;
		rdata[1].data = (char *) GinDataLeafPageGetPostingList(lpage);
		rdata[1].len = lsize;
		rdata[1].next = &rdata[2];

		rdata[2].buffer = InvalidBuffer;
		rdata[2].data = (char *) GinDataLeafPageGetPostingList(rpage);
		rdata[2].len = rsize;
		rdata[2].next = NULL;

		*newlpage = lpage;
		*newrpage = rpage;

		wassplit = true;

		elog(DEBUG2, "inserted %d items to block %u; split %d/%d",
			 maxitems, BufferGetBlockNumber(buf), (int) lsize, (int) rsize);
	}

	items->curitem += maxitems;

	return !wassplit;
}

/*
 * Place a PostingItem to page, and fill a WAL record.
 *
 * If the item doesn't fit, returns false without modifying the page.
 *
 * In addition to inserting the given item, the downlink of the existing item
 * at 'off' is updated to point to 'updateblkno'.
 */
static bool
dataPlaceToPageInternal(GinBtree btree, Buffer buf, GinBtreeStack *stack,
						void *insertdata, BlockNumber updateblkno,
						XLogRecData **prdata, Page *newlpage, Page *newrpage)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber off = stack->off;
	PostingItem *pitem;
	/* these must be static so they can be returned to caller */
	static XLogRecData rdata;
	static ginxlogInsertDataInternal data;

	/* split if we have to */
	if (GinNonLeafDataPageGetFreeSpace(page) < sizeof(PostingItem))
	{
		dataSplitPageInternal(btree, buf, stack, insertdata, updateblkno,
							  prdata, newlpage, newrpage);
		return false;
	}

	*prdata = &rdata;
	Assert(GinPageIsData(page));

	START_CRIT_SECTION();

	/* Update existing downlink to point to next page (on internal page) */
	pitem = GinDataPageGetPostingItem(page, off);
	PostingItemSetBlockNumber(pitem, updateblkno);

	/* Add new item */
	pitem = (PostingItem *) insertdata;
	GinDataPageAddPostingItem(page, pitem, off);

	data.offset = off;
	data.newitem = *pitem;

	rdata.buffer = buf;
	rdata.buffer_std = false;
	rdata.data = (char *) &data;
	rdata.len = sizeof(ginxlogInsertDataInternal);
	rdata.next = NULL;

	return true;
}

/*
 * Places an item (or items) to a posting tree. Calls relevant function of
 * internal of leaf page because they are handled very differently.
 */
static bool
dataPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
				void *insertdata, BlockNumber updateblkno,
				XLogRecData **prdata,
				Page *newlpage, Page *newrpage)
{
	Page		page = BufferGetPage(buf);

	Assert(GinPageIsData(page));

	if (GinPageIsLeaf(page))
		return dataPlaceToPageLeaf(btree, buf, stack, insertdata,
								   prdata, newlpage, newrpage);
	else
	{
		return dataPlaceToPageInternal(btree, buf, stack,
									   insertdata, updateblkno,
									   prdata, newlpage, newrpage);
	}
}

/*
 * Split page and fill WAL record. Returns a new temp buffer filled with data
 * that should go to the left page. The original buffer is left untouched.
 */
static void
dataSplitPageInternal(GinBtree btree, Buffer origbuf,
					  GinBtreeStack *stack,
					  void *insertdata, BlockNumber updateblkno,
					  XLogRecData **prdata, Page *newlpage, Page *newrpage)
{
	Page		oldpage = BufferGetPage(origbuf);
	OffsetNumber off = stack->off;
	int			nitems = GinPageGetOpaque(oldpage)->maxoff;
	Size		pageSize = PageGetPageSize(oldpage);
	ItemPointerData oldbound = *GinDataPageGetRightBound(oldpage);
	ItemPointer	bound;
	Page		lpage;
	Page		rpage;
	OffsetNumber separator;

	/* these must be static so they can be returned to caller */
	static ginxlogSplitDataInternal data;
	static XLogRecData rdata[4];
	static PostingItem allitems[(BLCKSZ / sizeof(PostingItem)) + 1];

	lpage = PageGetTempPage(oldpage);
	rpage = PageGetTempPage(oldpage);
	GinInitPage(rpage, GinPageGetOpaque(rpage)->flags, pageSize);

	*prdata = rdata;

	/*
	 * First construct a new list of PostingItems, which includes all the
	 * old items, and the new item.
	 */
	memcpy(allitems, GinDataPageGetPostingItem(oldpage, FirstOffsetNumber),
		   (off - 1) * sizeof(PostingItem));

	allitems[off - 1] = *((PostingItem *) insertdata);
	memcpy(&allitems[off], GinDataPageGetPostingItem(oldpage, off),
		   (nitems - (off - 1)) * sizeof(PostingItem));
	nitems++;

	/* Update existing downlink to point to next page */
	PostingItemSetBlockNumber(&allitems[off], updateblkno);

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

	data.separator = separator;
	data.nitem = nitems;
	data.rightbound = oldbound;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(ginxlogSplitDataInternal);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = (char *) allitems;
	rdata[1].len = nitems * sizeof(PostingItem);
	rdata[1].next = NULL;

	*newlpage = lpage;
	*newrpage = rpage;
}

/*
 * Construct insertion payload for inserting the downlink for given buffer.
 */
static void *
dataPrepareDownlink(GinBtree btree, Buffer lbuf)
{
	PostingItem *pitem = palloc(sizeof(PostingItem));
	Page		lpage = BufferGetPage(lbuf);

	PostingItemSetBlockNumber(pitem, BufferGetBlockNumber(lbuf));
	pitem->key = *GinDataPageGetRightBound(lpage);

	return pitem;
}

/*
 * Fills new root by right bound values from child.
 * Also called from ginxlog, should not use btree
 */
void
ginDataFillRoot(GinBtree btree, Page root, BlockNumber lblkno, Page lpage, BlockNumber rblkno, Page rpage)
{
	PostingItem li,
				ri;

	li.key = *GinDataPageGetRightBound(lpage);
	PostingItemSetBlockNumber(&li, lblkno);
	GinDataPageAddPostingItem(root, &li, InvalidOffsetNumber);

	ri.key = *GinDataPageGetRightBound(rpage);
	PostingItemSetBlockNumber(&ri, rblkno);
	GinDataPageAddPostingItem(root, &ri, InvalidOffsetNumber);
}

/*** Functions that are exported to the rest of the GIN code ***/

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
	Pointer		ptr;
	PostingListSegment *segment;
	int			nrootitems;
	int			rootsize;

	/*
	 * Create the root page.
	 */
	buffer = GinNewBuffer(index);
	page = BufferGetPage(buffer);
	blkno = BufferGetBlockNumber(buffer);

	START_CRIT_SECTION();

	GinInitDataLeafPage(page);
	GinPageGetOpaque(page)->rightlink = InvalidBlockNumber;

	/*
	 * Write as many of the items to the root page as fit. Root page is divided
	 * in segments of GinPostingListSegmentMaxSize maximum size.
	 */
	elog(DEBUG2, "creating new GIN posting tree");

	nrootitems = 0;
	rootsize = 0;
	ptr = (Pointer)GinDataLeafPageGetPostingList(page);
	while (nrootitems < nitems)
	{
		int npacked, segsize;

		segment = ginCompressPostingList(&items[nrootitems],
										 nitems - nrootitems,
										 GinPostingListSegmentMaxSize,
										 &npacked);
		segsize = SizeOfPostingListSegment(segment);
		if (rootsize + segsize > GinDataLeafMaxPostingListSize)
			break;

		memcpy(ptr, segment, segsize);
		ptr += segsize;
		rootsize += segsize;
		Assert(rootsize <= GinDataLeafMaxPostingListSize);
		nrootitems += npacked;
		pfree(segment);
	}
	GinDataLeafPageSetPostingListSize(page, rootsize);
	MarkBufferDirty(buffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		ginxlogCreatePostingTree data;
		static char		buf[BLCKSZ];

		data.node = index->rd_node;
		data.blkno = blkno;
		data.size = rootsize;

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

	Assert(GinPageGetOpaque(page)->rightlink == InvalidBlockNumber);

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
		ginInsertItemPointers(index, blkno,
							  items + nrootitems,
							  nitems - nrootitems,
							  buildStats);
	}

	return blkno;
}

void
ginPrepareDataScan(GinBtree btree, Relation index, BlockNumber rootBlkno)
{
	memset(btree, 0, sizeof(GinBtreeData));

	btree->index = index;
	btree->rootBlkno = rootBlkno;

	btree->findChildPage = dataLocateItem;
	btree->getLeftMostChild = dataGetLeftMostPage;
	btree->isMoveRight = dataIsMoveRight;
	btree->findItem = dataLocateLeafItem;
	btree->findChildPtr = dataFindChildPtr;
	btree->placeToPage = dataPlaceToPage;
	btree->fillRoot = ginDataFillRoot;
	btree->prepareDownlink = dataPrepareDownlink;

	btree->isData = TRUE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;
}

/*
 * Inserts array of item pointers, may execute several tree scan (very rare)
 */
void
ginInsertItemPointers(Relation index, BlockNumber rootBlkno,
					  ItemPointerData *items, uint32 nitem,
					  GinStatsData *buildStats)
{
	GinBtreeData btree;
	GinBtreeDataLeafInsertData insertdata;
	GinBtreeStack *stack;

	ginPrepareDataScan(&btree, index, rootBlkno);
	btree.isBuild = (buildStats != NULL);
	insertdata.items = items;
	insertdata.nitem = nitem;
	insertdata.curitem = 0;

	while (insertdata.curitem < insertdata.nitem)
	{
		/* search for the leaf page where the first item should go to */
		btree.itemptr = insertdata.items[insertdata.curitem];
		stack = ginFindLeafPage(&btree, false);

		if (btree.findItem(&btree, stack))
		{
			/*
			 * Current item already exists in index.
			 */
			insertdata.curitem++;
			LockBuffer(stack->buffer, GIN_UNLOCK);
			freeGinBtreeStack(stack);
		}
		else
			ginInsertValue(&btree, stack, &insertdata, buildStats);
	}
}

/*
 * Starts a new scan on a posting tree.
 */
GinBtreeStack *
ginScanBeginPostingTree(Relation index, BlockNumber rootBlkno)
{
	GinBtreeData btree;
	GinBtreeStack *stack;

	ginPrepareDataScan(&btree, index, rootBlkno);

	btree.fullScan = TRUE;

	stack = ginFindLeafPage(&btree, TRUE);

	return stack;
}
