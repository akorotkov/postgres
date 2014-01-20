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
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Size of the posting lists stored on leaf pages, in bytes. The code can
 * deal with any size, but random access is more efficient when a number of
 * smaller lists are stored, rather than one big list.
 */
#define GinPostingListSegmentMaxSize 256

/*
 * At least this many items fit in a GinPostingListSegmentMaxSize-bytes
 * long segment. This is used when estimating how much space is required
 * for N items, at minimum.
 */
#define MinTuplesPerSegment ((GinPostingListSegmentMaxSize - 2) / 6)

static void GinInitDataLeafPage(Page page);
static int bsearch_itemptr(ItemPointer array, int nitems, ItemPointer key,
				bool *found);

static void dataSplitPageInternal(GinBtree btree, Buffer origbuf,
					  GinBtreeStack *stack,
					  void *insertdata, BlockNumber updateblkno,
					  XLogRecData **prdata, Page *newlpage, Page *newrpage);


/*
 * This struct holds some auxiliary information about posting list segments,
 * during leaf page insertion or split.
 */
typedef struct
{
	GinPostingList *seg;
	ItemPointerData maxitem;	/* largest itemptr in the segment, invalid if
								 * not known */
	bool		unmodified;		/* is this segment on page already? */
	bool		putright;		/* put this segment to right page */
} splitSegmentInfo;

static void dataPlaceToPageLeafUncompressed(GinBtree btree, Buffer buf,
								ItemPointer newItems, int maxitems,
								XLogRecData **prdata);
static void dataPlaceToPageLeafRecompress(GinBtree btree, Buffer buf,
							  splitSegmentInfo *splitinfo, int nsegments,
							  XLogRecData **prdata);
static void dataPlaceToPageLeafSplit(GinBtree btree,
						 splitSegmentInfo *splitinfo, int nsegments,
						 XLogRecData **prdata, Page lpage, Page rpage);

/*
 * Read all TIDs from leaf data page to single uncompressed array.
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

	/*
	 * Decode all the compressed lists on the page.
	 */
	if (GinPageIsCompressed(page))
	{
		GinPostingList *ptr = GinDataLeafPageGetPostingList(page);
		Size		len = GinDataLeafPageGetPostingListSize(page);

		compressed = ginPostingListDecodeAllSegments(ptr, len, &ncompressed);
	}
	else
	{
		compressed = NULL;
		ncompressed = 0;
	}

	uncompressed = GinDataLeafPageGetUncompressed(page, &nuncompressed);

	/* Merge the compressed and uncompressed lists */
	nall = nuncompressed + ncompressed;
	if (nuncompressed > 0 && ncompressed > 0)
	{
		all = (ItemPointer) palloc(nall * sizeof(ItemPointerData));
		nall = ginMergeItemPointers(all,
									compressed, ncompressed,
									uncompressed, nuncompressed);
		pfree(compressed);
	}
	else if (nuncompressed > 0)
	{
		/*
		 * GinDataLeafPageGetUncompressed returns a straight pointer to the
		 * page, so make a palloc'd copy of it
		 */
		all = (ItemPointer) palloc(nuncompressed * sizeof(ItemPointerData));
		memcpy(all, uncompressed, nuncompressed * sizeof(ItemPointerData));
	}
	else if (ncompressed > 0)
		all = compressed;
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

	/*
	 * Decode all the compressed lists on the page.
	 */
	if (GinPageIsCompressed(page))
	{
		GinPostingList *segment = GinDataLeafPageGetPostingList(page);
		Size		len = GinDataLeafPageGetPostingListSize(page);

		nall += ginPostingListDecodeAllSegmentsToTbm(segment, len, tbm);
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
 * Get pointer to the uncompressed array of of items on a leaf page. The
 * number of items in the array is returned in *nitems.
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
		/*
		 * In the old pre-9.4 page format, the whole page content is used for
		 * uncompressed items, and the number of items is stored in 'maxoff'
		 */
		items = (ItemPointer) GinDataPageGetData(page);
		*nitems = GinPageGetOpaque(page)->maxoff;
	}

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
 * Find correct PostingItem in non-leaf page. It is assumed that this is
 * the correct page, and the searched value SHOULD be on the page.
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
 * Returns true if the given item (btree->itemptr) is on the current page
 * (stack->buffer).
 */
static bool
dataLocateLeafItem(GinBtree btree, GinBtreeStack *stack)
{
	Page			page = BufferGetPage(stack->buffer);
	bool			result;
	ItemPointer		items;
	int				nitems;

	Assert(GinPageIsData(page) && GinPageIsLeaf(page));

	/*
	 * First check if the item is in the uncompressed part of the page.
	 */
	items = GinDataLeafPageGetUncompressed(page, &nitems);

	if (nitems > 0)
	{
		bsearch_itemptr(items, nitems, &btree->itemptr, &result);
		if (result)
			return true;
	}

	/*
	 * Not found. Search the compressed lists.
	 */
	if (GinPageIsCompressed(page))
	{
		/*
		 * Walk the segments until we find the one that should contain the
		 * item we're searching for.
		 */
		GinPostingList *segment;
		char	   *endPtr;

		segment = GinDataLeafPageGetPostingList(page);
		endPtr =  (char *) segment + GinDataLeafPageGetPostingListSize(page);
		while (ginCompareItemPointers(&segment->first, &btree->itemptr) < 0)
		{
			GinPostingList *nextsegment = GinNextPostingListSegment(segment);
			if ((char *) nextsegment >= endPtr)
			{
				/* this was the last segment */
				Assert ((char *) nextsegment == endPtr);
				break;
			}
			segment = (GinPostingList *) nextsegment;
		}
		Assert((char *) segment <= endPtr);

		/* Scan the segment */
		items = ginPostingListDecode(segment, &nitems);
		(void) bsearch_itemptr(items, nitems, &btree->itemptr, &result);
		pfree(items);
	}

	return result;
}

/*
 * Find link to blkno on non-leaf page, returns offset of PostingItem
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
 * Return blkno of leftmost child
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
 * Add PostingItem to a non-leaf page.
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
 * Delete posting item from non-leaf page
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
	int			i;
	ItemPointerData	rbound;
	ItemPointer olditems;
	int			nolditems;
	ItemPointer allitems;
	int			nallitems;
	bool		needsplit;
	bool		append;
	int			totalpacked;
	Size		lsize = 0;
	Size		pgused;
	GinPostingList *seg;
	int			npacked;
	int			lastleft = 0;
	int			segsize;
	ItemPointer olduncompressed;
	int			nolduncompressed;
	Size		freespace;
	Pointer		segend;
	Pointer		segbegin;
	ItemPointerData minNewItem;
	MemoryContext tmpCxt;
	MemoryContext oldCxt;
	/*
	 * One entry for each segment we'll have after re-encoding. Has to be
	 * large enough to hold all segments, even if some of them are somewhat
	 * shorter than the the maximum. We never create segments smaller than
	 * 1/2 the maximum size.
	 */
	splitSegmentInfo splitinfo[((GinDataLeafMaxContentSize / GinPostingListSegmentMaxSize) + 1) * 4];
	int			nsegments;

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
	 * re-encode the page if it was in the old pre-9.4 format.
	 */
	if (GinPageIsCompressed(page))
		freespace = GinDataLeafPageGetFreeSpace(page);
	else
		freespace = 0;

	if (freespace >= sizeof(ItemPointerData) * maxitems)
	{
		/*
		 * Great, the new items fit in the uncompressed area. Insert them and
		 * we're done.
		 *
		 * Once we start modifying the page, there's no turning back. The
		 * caller is responsible for calling END_CRIT_SECTION() after writing
		 * the WAL record.
		 */
		START_CRIT_SECTION();
		dataPlaceToPageLeafUncompressed(btree, buf,
										newItems, maxitems, prdata);
		items->curitem += maxitems;

		elog(DEBUG2, "inserted %d items to block %u; fit uncompressed",
			 maxitems, BufferGetBlockNumber(buf));
		return true;
	}

	/*
	 * Didn't fit uncompressed. We'll have to compress.
	 *
	 * The following operations do quite a lot of small memory allocations,
	 * create a temporary memory context so that we don't need to keep track
	 * of them individually.
	 */
	tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
								   "Gin split temporary context",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldCxt = MemoryContextSwitchTo(tmpCxt);

	/*
	 * Existing segments that only contain items < the new items don't need
	 * to be modified. The rest are decoded and merged into a single array
	 * that contains all the new items we're about to pack and store on the
	 * page. That includes newly inserted items, any existing uncompressed
	 * items on the page that we will compress, and items from existing
	 * segments that need to be re-encoded.
	 */
	if (nolduncompressed > 0 &&
			ginCompareItemPointers(&olduncompressed[0], &newItems[0]) < 0)
		minNewItem = olduncompressed[0];
	else
		minNewItem = newItems[0];

	/*
	 * Fill in a splitSegmentInfo entry for each old segment we'll keep
	 * unmodified.
	 */
	nsegments = 0;
	seg = GinDataLeafPageGetPostingList(page);
	segbegin = (Pointer) seg;
	segend = segbegin + GinDataLeafPageGetPostingListSize(page);
	pgused = 0;
	while ((Pointer) seg < segend)
	{
		if (ginCompareItemPointers(&minNewItem, &seg->first) < 0)
			break;

		/*
		 * Force recompression of abnormally small segments. (VACUUM can leave
		 * these behind)
		 */
		if (SizeOfGinPostingList(seg) < (GinPostingListSegmentMaxSize * 2) / 3)
			break;

		splitinfo[nsegments].seg = seg;
		ItemPointerSetInvalid(&splitinfo[nsegments].maxitem);
		splitinfo[nsegments].unmodified = true;
		splitinfo[nsegments].putright = false;
		nsegments++;
		pgused += SizeOfGinPostingList(seg);
		seg = GinNextPostingListSegment(seg);
	}

	/*
	 * if we have to re-encode any existing compressed items, merge them with
	 * the old uncompressed items.
	 */
	if ((Pointer) seg != segend)
	{
		ItemPointer tmp;

		olditems = ginPostingListDecodeAllSegments(seg,
												   segend - (Pointer) seg, &nolditems);
		tmp = palloc(sizeof(ItemPointerData) * (nolduncompressed + nolditems));
		nolditems = ginMergeItemPointers(tmp,
										 olditems, nolditems,
										 olduncompressed, nolduncompressed);
		pfree(olditems);
		olditems = tmp;
	}
	else
	{
		olditems = olduncompressed;
		nolditems = nolduncompressed;
	}

	/* Are we appending to the end of the page? */
	append = (nolditems == 0 ||
			  ginCompareItemPointers(&newItems[0], &olditems[nolditems - 1]) > 0);

	/*
	 * If we're appending to the end of the page, append as many items as we
	 * can fit (after splitting), and stop when the pages becomes full.
	 * Otherwise we have to limit the number of new items to insert, because
	 * once we start packing we can't just stop when we run out of space,
	 * because we must make sure that all the old items still fit.
	 */
	if (append)
	{
		/*
		 * Even when appending, trying to append more items than will fit is
		 * not completely free, because we will merge the new items and old
		 * items into an array below. In the best case, every new item fits in
		 * a single byte, and we can use all the free space on the old page as
		 * well as the new page. For simplicity, ignore segment overhead etc.
		 */
		maxitems = Min(maxitems, freespace + GinDataLeafMaxContentSize);
	}
	else
	{
		/*
		 * Calculate a conservative estimate of how many new items we can fit
		 * on the two pages after splitting.
		 *
		 * We can use any remaining free space on the old page to store full
		 * segments, as well as the new page. Each full-sized segment can hold
		 * at least MinTuplesPerSegment items
		 */
		int			nnewsegments;

		nnewsegments = freespace / GinPostingListSegmentMaxSize;
		nnewsegments += GinDataLeafMaxContentSize / GinPostingListSegmentMaxSize;
		maxitems = Min(maxitems, nnewsegments * MinTuplesPerSegment);
	}

	/*
	 * Merge the old and new items into one array.
	 */
	if (nolditems > 0)
	{
		allitems = palloc((nolditems + maxitems) * sizeof(ItemPointerData));
		nallitems = ginMergeItemPointers(allitems,
										 olditems, nolditems,
										 newItems, maxitems);
	}
	else
	{
		allitems = newItems;
		nallitems = maxitems;
	}

	/*
	 * We now have all the items we're about to add in 'allitems'.
	 *
	 * Start packing them into segments. Stop when we have consumed enough
	 * space to fill both pages, or we run out of items.
	 */
	totalpacked = 0;
	needsplit = false;
	lastleft = nsegments;
	while (totalpacked < nallitems)
	{
		seg = ginCompressPostingList(&allitems[totalpacked],
									 nallitems - totalpacked,
									 GinPostingListSegmentMaxSize,
									 &npacked);
		segsize = SizeOfGinPostingList(seg);
		if (pgused + segsize > GinDataLeafMaxContentSize)
		{
			if (!needsplit)
			{
				/* switch to right page */
				Assert(nsegments > 0);
				lastleft = nsegments - 1;
				needsplit = true;
				lsize = pgused;
				pgused = 0;
			}
			else if (append)
			{
				/*
				 * adjust maxitems for the number of new items actually
				 * appended.
				 */
				maxitems -= nallitems - totalpacked;
				break;
			}
			else
				elog(ERROR, "could not split GIN page, didn't fit");
		}

		splitinfo[nsegments].seg = seg;
		splitinfo[nsegments].maxitem = allitems[totalpacked + npacked - 1];
		splitinfo[nsegments].unmodified = false;
		splitinfo[nsegments].putright = needsplit;
		nsegments++;

		pgused += segsize;
		totalpacked += npacked;
	}

	if (!needsplit)
	{
		/*
		 * Great, all the items fit on a single page. Write the segments to
		 * the page, and WAL-log appropriately.
		 *
		 * Once we start modifying the page, there's no turning back. The
		 * caller is responsible for calling END_CRIT_SECTION() after writing
		 * the WAL record.
		 */
		Assert(totalpacked == nallitems);

		START_CRIT_SECTION();
		dataPlaceToPageLeafRecompress(btree, buf,
									  splitinfo, nsegments,
									  prdata);

		elog(DEBUG2, "inserted %d items to block %u; re-encoded %d/%d to %d bytes",
			 maxitems, BufferGetBlockNumber(buf),
			 nolduncompressed, nolditems,
			 (int) pgused);
	}
	else
	{
		/*
		 * Had to split.
		 */
		Size		rsize = pgused;

		Assert(lsize <= GinDataLeafMaxContentSize);
		Assert(rsize <= GinDataLeafMaxContentSize);

		/*
		 * We already divided the segments between the left and the right
		 * page. The left page was filled as full as possible, and the rest
		 * overflowed to the right page. When building a new index, that's
		 * good, because the table is scanned from beginning to end and there
		 * won't be any more insertions to the left page during the build.
		 * This packs the index as tight as possible. But otherwise, split
		 * 50/50, by moving segments from the left page to the right page
		 * until they're balanced.
		 */
		if (!btree->isBuild)
		{
			while (lastleft >= 0)
			{
				segsize = SizeOfGinPostingList(splitinfo[lastleft].seg);
				if (lsize - segsize < rsize + segsize)
					break;

				splitinfo[lastleft].putright = true;
				 /* don't consider segments moved to right as unmodified */
				splitinfo[lastleft].unmodified = false;
				lsize -= segsize;
				rsize += segsize;
				lastleft--;
			}
		}
		Assert(lsize <= GinDataLeafMaxContentSize);
		Assert(rsize <= GinDataLeafMaxContentSize);

		/*
		 * Make sure the max item in the left page's last segment is known;
		 * it becomes the right bound of the page.
		 */
		if (!ItemPointerIsValid(&splitinfo[lastleft].maxitem))
		{
			ItemPointer	tmpitems;
			int			tmpcount;

			tmpitems = ginPostingListDecode(splitinfo[lastleft].seg, &tmpcount);
			splitinfo[lastleft].maxitem = tmpitems[tmpcount - 1];
			pfree(tmpitems);
		}

		*newlpage = MemoryContextAlloc(oldCxt, BLCKSZ);
		*newrpage = MemoryContextAlloc(oldCxt, BLCKSZ);

		dataPlaceToPageLeafSplit(btree, splitinfo, nsegments,
								 prdata, *newlpage, *newrpage);

		Assert(GinPageRightMost(page) ||
			   ginCompareItemPointers(GinDataPageGetRightBound(*newlpage),
									  GinDataPageGetRightBound(*newrpage)) < 0);

		elog(DEBUG2, "inserted %d items to block %u; split %d/%d",
			 maxitems, BufferGetBlockNumber(buf), (int) lsize, (int) rsize);
	}

	items->curitem += maxitems;

	MemoryContextSwitchTo(oldCxt);
	MemoryContextDelete(tmpCxt);

	return !needsplit;
}

/*
 * Subroutine of dataPlaceToPageLeaf(). Inserts new items into the
 * uncompressed area of a page.
 */
static void
dataPlaceToPageLeafUncompressed(GinBtree btree, Buffer buf,
								ItemPointer newItems, int maxitems,
								XLogRecData **prdata)
{
	Page		page = BufferGetPage(buf);
	PageHeader	phdr = (PageHeader) page;
	int			upper;
	int			totalitems;
	ItemPointer olduncompressed;
	int			nolduncompressed;
	/*
	 * these must be static so they can be returned to caller (no pallocs
	 * since we're in a critical section!)
	 */
	static ginxlogInsertDataLeafUncompressed insert_xlog;
	static XLogRecData rdata[2];

	olduncompressed = GinDataLeafPageGetUncompressed(page, &nolduncompressed);

	/* Merge the new items into the array on page */
	upper = phdr->pd_upper - sizeof(ItemPointerData) * maxitems;
	totalitems = ginMergeItemPointers((ItemPointer) (page + upper),
									  olduncompressed, nolduncompressed,
									  newItems, maxitems);
	/*
	 * We're not expecting duplicates. But we've already modified the page
	 * so there's no turning back.
	 */
	if (totalitems != nolduncompressed + maxitems)
		elog(PANIC, "cannot insert duplicate items to GIN index page");

	phdr->pd_upper = upper;
	Assert(phdr->pd_upper >= phdr->pd_lower);

	/* Put WAL data */
	insert_xlog.length = sizeof(ItemPointerData) * maxitems;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &insert_xlog;
	rdata[0].len = offsetof(ginxlogInsertDataLeafUncompressed, newitems);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = buf;
	rdata[1].buffer_std = TRUE;
	rdata[1].data = (char *) newItems;
	rdata[1].len = sizeof(ItemPointerData) * maxitems;
	rdata[1].next = NULL;

	*prdata = rdata;
}

/*
 * Subroutine of dataPlaceToPageLeaf(). Replaces and/or appends new compressed
 * segments to page.
 */
static void
dataPlaceToPageLeafRecompress(GinBtree btree, Buffer buf,
							  splitSegmentInfo *splitinfo, int nsegments,
							  XLogRecData **prdata)
{
	Page		page = BufferGetPage(buf);
	char	   *ptr;
	int			newsize;
	int			unmodifiedsize;
	int			i;
	bool		modified = false;
	/*
	 * these must be static so they can be returned to caller (no pallocs
	 * since we're in a critical section!)
	 */
	static ginxlogInsertDataLeafCompressed insert_xlog;
	static XLogRecData rdata[2];

	ptr = (char *) GinDataLeafPageGetPostingList(page);
	newsize = 0;
	unmodifiedsize = 0;
	modified = false;
	for (i = 0; i < nsegments; i++)
	{
		GinPostingList *seg = splitinfo[i].seg;
		int			segsize = SizeOfGinPostingList(seg);

		if (!splitinfo[i].unmodified)
			modified = true;

		if (!modified)
			unmodifiedsize += segsize;
		else
			memcpy(ptr, splitinfo[i].seg, segsize);
		ptr += segsize;
		newsize += segsize;
	}
	GinDataLeafPageSetPostingListSize(page, newsize);

	GinPageSetCompressed(page);
	((PageHeader) page)->pd_upper = ((PageHeader) page)->pd_special;

	/* Put WAL data */
	insert_xlog.length = INSERT_RECOMPRESS_FLAG | (uint16) newsize;
	insert_xlog.unmodifiedsize = unmodifiedsize;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &insert_xlog;
	rdata[0].len = offsetof(ginxlogInsertDataLeafCompressed, newdata);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = buf;
	rdata[1].buffer_std = TRUE;
	rdata[1].data = ((char *) GinDataLeafPageGetPostingList(page)) + unmodifiedsize;
	rdata[1].len = newsize - unmodifiedsize;
	rdata[1].next = NULL;

	*prdata = rdata;
}

/*
 * Subroutine of dataPlaceToPageLeaf(). Splits a page, and replaces and/or
 * appends new compressed segments to the pages.
 *
 * This is different from the non-split cases in that this does not modify
 * the pages directly, but returns temporary copies of the new left and right
 * pages. Hence this is not called in a critical section, which makes it safe
 * to e.g palloc.
 */
static void
dataPlaceToPageLeafSplit(GinBtree btree,
						 splitSegmentInfo *splitinfo, int nsegments,
						 XLogRecData **prdata, Page lpage, Page rpage)
{
	char	   *ptr;
	int			segsize;
	int			lsize;
	int			rsize;
	int			unmodifiedsize;
	int			i;
	bool		modified = false;
	/* these must be static so they can be returned to caller */
	static ginxlogSplitDataLeaf split_xlog;
	static XLogRecData rdata[3];

	/* Initialize temporary pages to hold the new left and right pages */
	GinInitDataLeafPage(lpage);
	GinInitDataLeafPage(rpage);

	/* Copy the segments that go to the left page */
	lsize = 0;
	ptr = (char *) GinDataLeafPageGetPostingList(lpage);
	unmodifiedsize = 0;
	modified = false;
	for (i = 0; i < nsegments && !splitinfo[i].putright; i++)
	{
		segsize = SizeOfGinPostingList(splitinfo[i].seg);

		if (!splitinfo[i].unmodified)
			modified = true;

		if (!modified)
			unmodifiedsize += segsize;

		/*
		 * We're building temporary in-memory pages, so we have to copy
		 * unmodified items too.
		 */
		memcpy(ptr, splitinfo[i].seg, segsize);
		ptr += segsize;
		lsize += segsize;
	}
	GinDataLeafPageSetPostingListSize(lpage, lsize);
	*GinDataPageGetRightBound(lpage) = splitinfo[i - 1].maxitem;

	/* Copy the segments that go to the right page */
	ptr = (char *) GinDataLeafPageGetPostingList(rpage);
	rsize = 0;
	for (;i < nsegments; i++)
	{
		Assert(splitinfo[i].putright);
		segsize = SizeOfGinPostingList(splitinfo[i].seg);

		memcpy(ptr, splitinfo[i].seg, segsize);
		ptr += segsize;
		rsize += segsize;
	}
	GinDataLeafPageSetPostingListSize(rpage, rsize);
	*GinDataPageGetRightBound(rpage) = splitinfo[i - 1].maxitem;

	/* Create WAL record */
	split_xlog.lsize = lsize;
	split_xlog.unmodifiedsize = unmodifiedsize;
	split_xlog.rsize = rsize;
	split_xlog.lrightbound = *GinDataPageGetRightBound(lpage);
	split_xlog.rrightbound = *GinDataPageGetRightBound(rpage);

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &split_xlog;
	rdata[0].len = sizeof(ginxlogSplitDataLeaf);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = (char *) GinDataLeafPageGetPostingList(lpage) + unmodifiedsize;
	rdata[1].len = lsize - unmodifiedsize;
	rdata[1].next = &rdata[2];

	rdata[2].buffer = InvalidBuffer;
	rdata[2].data = (char *) GinDataLeafPageGetPostingList(rpage);
	rdata[2].len = rsize;
	rdata[2].next = NULL;

	*prdata = rdata;
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
		return dataPlaceToPageInternal(btree, buf, stack,
									   insertdata, updateblkno,
									   prdata, newlpage, newrpage);
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
	 * Write as many of the items to the root page as fit. In segments
	 * of max GinPostingListSegmentMaxSize bytes each.
	 */
	nrootitems = 0;
	rootsize = 0;
	ptr = (Pointer) GinDataLeafPageGetPostingList(page);
	while (nrootitems < nitems)
	{
		GinPostingList *segment;
		int			npacked;
		int			segsize;

		segment = ginCompressPostingList(&items[nrootitems],
										 nitems - nrootitems,
										 GinPostingListSegmentMaxSize,
										 &npacked);
		segsize = SizeOfGinPostingList(segment);
		if (rootsize + segsize > GinDataLeafMaxContentSize)
			break;

		memcpy(ptr, segment, segsize);
		ptr += segsize;
		rootsize += segsize;
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

		data.node = index->rd_node;
		data.blkno = blkno;
		data.size = rootsize;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &data;
		rdata[0].len = sizeof(ginxlogCreatePostingTree);
		rdata[0].next = &rdata[1];

		rdata[1].buffer = InvalidBuffer;
		rdata[1].data = (char *) GinDataLeafPageGetPostingList(page);
		rdata[1].len = rootsize;
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
