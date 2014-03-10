/*-------------------------------------------------------------------------
 *
 * vodkadatapage.c
 *	  routines for handling VODKA posting tree pages.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkadatapage.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/vodka_private.h"
#include "access/heapam_xlog.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Size of the posting lists stored on leaf pages, in bytes. The code can
 * deal with any size, but random access is more efficient when a number of
 * smaller lists are stored, rather than one big list.
 */
#define VodkaPostingListSegmentMaxSize 256

/*
 * Existing posting lists smaller than this are recompressed, when inserting
 * new items to page.
 */
#define VodkaPostingListSegmentMinSize 192

/*
 * At least this many items fit in a VodkaPostingListSegmentMaxSize-bytes
 * long segment. This is used when estimating how much space is required
 * for N items, at minimum.
 */
#define MinTuplesPerSegment ((VodkaPostingListSegmentMaxSize - 2) / 6)

/*
 * A working struct for manipulating a posting tree leaf page.
 */
typedef struct
{
	dlist_head	segments;		/* a list of leafSegmentInfos */

	/*
	 * The following fields represent how the segments are split across
	 * pages, if a page split is required. Filled in by leafRepackItems.
	 */
	dlist_node *lastleft;		/* last segment on left page */
	int			lsize;			/* total size on left page */
	int			rsize;			/* total size on right page */
} disassembledLeaf;

typedef struct
{
	dlist_node	node;		/* linked list pointers */

	/*
	 * The following fields represent the items in this segment.
	 * If 'items' is not NULL, it contains a palloc'd array of the items
	 * in this segment. If 'seg' is not NULL, it contains the items in an
	 * already-compressed format. It can point to an on-disk page (!modified),
	 * or a palloc'd segment in memory. If both are set, they must represent
	 * the same items.
	 */
	VodkaPostingList *seg;
	ItemPointer items;
	int			nitems;			/* # of items in 'items', if items != NULL */

	bool		modified;		/* is this segment on page already? */
} leafSegmentInfo;

static ItemPointer dataLeafPageGetUncompressed(Page page, int *nitems);
static void dataSplitPageInternal(VodkaBtree btree, Buffer origbuf,
					  VodkaBtreeStack *stack,
					  void *insertdata, BlockNumber updateblkno,
					  XLogRecData **prdata, Page *newlpage, Page *newrpage);

static disassembledLeaf *disassembleLeaf(Page page);
static bool leafRepackItems(disassembledLeaf *leaf, ItemPointer remaining);
static bool addItemsToLeaf(disassembledLeaf *leaf, ItemPointer newItems, int nNewItems);


static void dataPlaceToPageLeafRecompress(Buffer buf,
							  disassembledLeaf *leaf,
							  XLogRecData **prdata);
static void dataPlaceToPageLeafSplit(Buffer buf,
						 disassembledLeaf *leaf,
						 ItemPointerData lbound, ItemPointerData rbound,
						 XLogRecData **prdata, Page lpage, Page rpage);

/*
 * Read TIDs from leaf data page to single uncompressed array. The TIDs are
 * returned in ascending order.
 *
 * advancePast is a hint, indicating that the caller is only interested in
 * TIDs > advancePast. To return all items, use ItemPointerSetMin.
 *
 * Note: This function can still return items smaller than advancePast that
 * are in the same posting list as the items of interest, so the caller must
 * still check all the returned items. But passing it allows this function to
 * skip whole posting lists.
 */
ItemPointer
VodkaDataLeafPageGetItems(Page page, int *nitems, ItemPointerData advancePast)
{
	ItemPointer result;

	if (VodkaPageIsCompressed(page))
	{
		VodkaPostingList *seg = VodkaDataLeafPageGetPostingList(page);
		Size		len = VodkaDataLeafPageGetPostingListSize(page);
		Pointer		endptr = ((Pointer) seg) + len;
		VodkaPostingList *next;

		/* Skip to the segment containing advancePast+1 */
		if (ItemPointerIsValid(&advancePast))
		{
			next = VodkaNextPostingListSegment(seg);
			while ((Pointer) next < endptr &&
				   vodkaCompareItemPointers(&next->first, &advancePast) <= 0)
			{
				seg = next;
				next = VodkaNextPostingListSegment(seg);
			}
			len = endptr - (Pointer) seg;
		}

		if (len > 0)
			result = vodkaPostingListDecodeAllSegments(seg, len, nitems);
		else
		{
			result = NULL;
			*nitems = 0;
		}
	}
	else
	{
		ItemPointer tmp = dataLeafPageGetUncompressed(page, nitems);

		result = palloc((*nitems) * sizeof(ItemPointerData));
		memcpy(result, tmp, (*nitems) * sizeof(ItemPointerData));
	}

	return result;
}

/*
 * Places all TIDs from leaf data page to bitmap.
 */
int
VodkaDataLeafPageGetItemsToTbm(Page page, TIDBitmap *tbm)
{
	ItemPointer uncompressed;
	int			nitems;

	if (VodkaPageIsCompressed(page))
	{
		VodkaPostingList *segment = VodkaDataLeafPageGetPostingList(page);
		Size		len = VodkaDataLeafPageGetPostingListSize(page);

		nitems = vodkaPostingListDecodeAllSegmentsToTbm(segment, len, tbm);
	}
	else
	{
		uncompressed = dataLeafPageGetUncompressed(page, &nitems);

		if (nitems > 0)
			tbm_add_tuples(tbm, uncompressed, nitems, false);
	}

	return nitems;
}

/*
 * Get pointer to the uncompressed array of items on a pre-9.4 format
 * uncompressed leaf page. The number of items in the array is returned in
 * *nitems.
 */
static ItemPointer
dataLeafPageGetUncompressed(Page page, int *nitems)
{
	ItemPointer items;

	Assert(!VodkaPageIsCompressed(page));

	/*
	 * In the old pre-9.4 page format, the whole page content is used for
	 * uncompressed items, and the number of items is stored in 'maxoff'
	 */
	items = (ItemPointer) VodkaDataPageGetData(page);
	*nitems = VodkaPageGetOpaque(page)->maxoff;

	return items;
}

/*
 * Check if we should follow the right link to find the item we're searching
 * for.
 *
 * Compares inserting item pointer with the right bound of the current page.
 */
static bool
dataIsMoveRight(VodkaBtree btree, Page page)
{
	ItemPointer iptr = VodkaDataPageGetRightBound(page);

	if (VodkaPageRightMost(page))
		return FALSE;

	return (vodkaCompareItemPointers(&btree->itemptr, iptr) > 0) ? TRUE : FALSE;
}

/*
 * Find correct PostingItem in non-leaf page. It is assumed that this is
 * the correct page, and the searched value SHOULD be on the page.
 */
static BlockNumber
dataLocateItem(VodkaBtree btree, VodkaBtreeStack *stack)
{
	OffsetNumber low,
				high,
				maxoff;
	PostingItem *pitem = NULL;
	int			result;
	Page		page = BufferGetPage(stack->buffer);

	Assert(!VodkaPageIsLeaf(page));
	Assert(VodkaPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		stack->predictNumber *= VodkaPageGetOpaque(page)->maxoff;
		return btree->getLeftMostChild(btree, page);
	}

	low = FirstOffsetNumber;
	maxoff = high = VodkaPageGetOpaque(page)->maxoff;
	Assert(high >= low);

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		pitem = VodkaDataPageGetPostingItem(page, mid);

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
			pitem = VodkaDataPageGetPostingItem(page, mid);
			result = vodkaCompareItemPointers(&btree->itemptr, &(pitem->key));
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
	pitem = VodkaDataPageGetPostingItem(page, high);
	return PostingItemGetBlockNumber(pitem);
}

/*
 * Find link to blkno on non-leaf page, returns offset of PostingItem
 */
static OffsetNumber
dataFindChildPtr(VodkaBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
{
	OffsetNumber i,
				maxoff = VodkaPageGetOpaque(page)->maxoff;
	PostingItem *pitem;

	Assert(!VodkaPageIsLeaf(page));
	Assert(VodkaPageIsData(page));

	/* if page isn't changed, we return storedOff */
	if (storedOff >= FirstOffsetNumber && storedOff <= maxoff)
	{
		pitem = VodkaDataPageGetPostingItem(page, storedOff);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return storedOff;

		/*
		 * we hope, that needed pointer goes to right. It's true if there
		 * wasn't a deletion
		 */
		for (i = storedOff + 1; i <= maxoff; i++)
		{
			pitem = VodkaDataPageGetPostingItem(page, i);
			if (PostingItemGetBlockNumber(pitem) == blkno)
				return i;
		}

		maxoff = storedOff - 1;
	}

	/* last chance */
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		pitem = VodkaDataPageGetPostingItem(page, i);
		if (PostingItemGetBlockNumber(pitem) == blkno)
			return i;
	}

	return InvalidOffsetNumber;
}

/*
 * Return blkno of leftmost child
 */
static BlockNumber
dataGetLeftMostPage(VodkaBtree btree, Page page)
{
	PostingItem *pitem;

	Assert(!VodkaPageIsLeaf(page));
	Assert(VodkaPageIsData(page));
	Assert(VodkaPageGetOpaque(page)->maxoff >= FirstOffsetNumber);

	pitem = VodkaDataPageGetPostingItem(page, FirstOffsetNumber);
	return PostingItemGetBlockNumber(pitem);
}

/*
 * Add PostingItem to a non-leaf page.
 */
void
VodkaDataPageAddPostingItem(Page page, PostingItem *data, OffsetNumber offset)
{
	OffsetNumber maxoff = VodkaPageGetOpaque(page)->maxoff;
	char	   *ptr;

	Assert(PostingItemGetBlockNumber(data) != InvalidBlockNumber);
	Assert(!VodkaPageIsLeaf(page));

	if (offset == InvalidOffsetNumber)
	{
		ptr = (char *) VodkaDataPageGetPostingItem(page, maxoff + 1);
	}
	else
	{
		ptr = (char *) VodkaDataPageGetPostingItem(page, offset);
		if (offset != maxoff + 1)
			memmove(ptr + sizeof(PostingItem),
					ptr,
					(maxoff - offset + 1) * sizeof(PostingItem));
	}
	memcpy(ptr, data, sizeof(PostingItem));

	VodkaPageGetOpaque(page)->maxoff++;
}

/*
 * Delete posting item from non-leaf page
 */
void
VodkaPageDeletePostingItem(Page page, OffsetNumber offset)
{
	OffsetNumber maxoff = VodkaPageGetOpaque(page)->maxoff;

	Assert(!VodkaPageIsLeaf(page));
	Assert(offset >= FirstOffsetNumber && offset <= maxoff);

	if (offset != maxoff)
		memmove(VodkaDataPageGetPostingItem(page, offset),
				VodkaDataPageGetPostingItem(page, offset + 1),
				sizeof(PostingItem) * (maxoff - offset));

	VodkaPageGetOpaque(page)->maxoff--;
}

/*
 * Places keys to leaf data page and fills WAL record.
 */
static VodkaPlaceToPageRC
dataPlaceToPageLeaf(VodkaBtree btree, Buffer buf, VodkaBtreeStack *stack,
					void *insertdata, XLogRecData **prdata,
					Page *newlpage, Page *newrpage)
{
	VodkaBtreeDataLeafInsertData *items = insertdata;
	ItemPointer newItems = &items->items[items->curitem];
	int			maxitems = items->nitem - items->curitem;
	Page		page = BufferGetPage(buf);
	int			i;
	ItemPointerData	rbound;
	ItemPointerData	lbound;
	bool		needsplit;
	bool		append;
	int			segsize;
	Size		freespace;
	MemoryContext tmpCxt;
	MemoryContext oldCxt;
	disassembledLeaf *leaf;
	leafSegmentInfo *lastleftinfo;
	ItemPointerData maxOldItem;
	ItemPointerData remaining;

	Assert(VodkaPageIsData(page));

	rbound  = *VodkaDataPageGetRightBound(page);

	/*
	 * Count how many of the new items belong to this page.
	 */
	if (!VodkaPageRightMost(page))
	{
		for (i = 0; i < maxitems; i++)
		{
			if (vodkaCompareItemPointers(&newItems[i], &rbound) > 0)
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

	/*
	 * The following operations do quite a lot of small memory allocations,
	 * create a temporary memory context so that we don't need to keep track
	 * of them individually.
	 */
	tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
								   "Vodka split temporary context",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldCxt = MemoryContextSwitchTo(tmpCxt);

	leaf = disassembleLeaf(page);

	/*
	 * Are we appending to the end of the page? IOW, are all the new items
	 * larger than any of the existing items.
	 */
	if (!dlist_is_empty(&leaf->segments))
	{
		lastleftinfo = dlist_container(leafSegmentInfo, node,
									   dlist_tail_node(&leaf->segments));
		if (!lastleftinfo->items)
			lastleftinfo->items = vodkaPostingListDecode(lastleftinfo->seg,
													   &lastleftinfo->nitems);
		maxOldItem = lastleftinfo->items[lastleftinfo->nitems - 1];
		if (vodkaCompareItemPointers(&newItems[0], &maxOldItem) >= 0)
			append = true;
		else
			append = false;
	}
	else
	{
		ItemPointerSetMin(&maxOldItem);
		append = true;
	}

	/*
	 * If we're appending to the end of the page, we will append as many items
	 * as we can fit (after splitting), and stop when the pages becomes full.
	 * Otherwise we have to limit the number of new items to insert, because
	 * once we start packing we can't just stop when we run out of space,
	 * because we must make sure that all the old items still fit.
	 */
	if (VodkaPageIsCompressed(page))
		freespace = VodkaDataLeafPageGetFreeSpace(page);
	else
		freespace = 0;
	if (append)
	{
		/*
		 * Even when appending, trying to append more items than will fit is
		 * not completely free, because we will merge the new items and old
		 * items into an array below. In the best case, every new item fits in
		 * a single byte, and we can use all the free space on the old page as
		 * well as the new page. For simplicity, ignore segment overhead etc.
		 */
		maxitems = Min(maxitems, freespace + VodkaDataLeafMaxContentSize);
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

		nnewsegments = freespace / VodkaPostingListSegmentMaxSize;
		nnewsegments += VodkaDataLeafMaxContentSize / VodkaPostingListSegmentMaxSize;
		maxitems = Min(maxitems, nnewsegments * MinTuplesPerSegment);
	}

	/* Add the new items to the segments */
	if (!addItemsToLeaf(leaf, newItems, maxitems))
	{
		 /* all items were duplicates, we have nothing to do */
		items->curitem += maxitems;

		MemoryContextSwitchTo(oldCxt);
		MemoryContextDelete(tmpCxt);

		return UNMODIFIED;
	}

	/*
	 * Pack the items back to compressed segments, ready for writing to disk.
	 */
	needsplit = leafRepackItems(leaf, &remaining);

	/*
	 * Did all the new items fit?
	 *
	 * If we're appending, it's OK if they didn't. But as a sanity check,
	 * verify that all the old items fit.
	 */
	if (ItemPointerIsValid(&remaining))
	{
		if (!append || ItemPointerCompare(&maxOldItem, &remaining) >= 0)
			elog(ERROR, "could not split VODKA page; all old items didn't fit");

		/* Count how many of the new items did fit. */
		for (i = 0; i < maxitems; i++)
		{
			if (vodkaCompareItemPointers(&newItems[i], &remaining) >= 0)
				break;
		}
		if (i == 0)
			elog(ERROR, "could not split VODKA page; no new items fit");
		maxitems = i;
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
		START_CRIT_SECTION();
		dataPlaceToPageLeafRecompress(buf, leaf, prdata);

		if (append)
			elog(DEBUG2, "appended %d new items to block %u; %d bytes (%d to go)",
				 maxitems, BufferGetBlockNumber(buf), (int) leaf->lsize,
				 items->nitem - items->curitem - maxitems);
		else
			elog(DEBUG2, "inserted %d new items to block %u; %d bytes (%d to go)",
				 maxitems, BufferGetBlockNumber(buf), (int) leaf->lsize,
				 items->nitem - items->curitem - maxitems);
	}
	else
	{
		/*
		 * Had to split.
		 *
		 * We already divided the segments between the left and the right
		 * page. The left page was filled as full as possible, and the rest
		 * overflowed to the right page. When building a new index, that's
		 * good, because the table is scanned from bevodkaning to end and there
		 * won't be any more insertions to the left page during the build.
		 * This packs the index as tight as possible. But otherwise, split
		 * 50/50, by moving segments from the left page to the right page
		 * until they're balanced.
		 *
		 * As a further heuristic, when appending items to the end of the
		 * page, split 75/25, one the assumption that subsequent insertions
		 * will probably also go to the end. This packs the index somewhat
		 * tighter when appending to a table, which is very common.
		 */
		if (!btree->isBuild)
		{
			while (dlist_has_prev(&leaf->segments, leaf->lastleft))
			{
				lastleftinfo = dlist_container(leafSegmentInfo, node, leaf->lastleft);

				segsize = SizeOfVodkaPostingList(lastleftinfo->seg);
				if (append)
				{
					if ((leaf->lsize - segsize) - (leaf->lsize - segsize) < BLCKSZ / 4)
						break;
				}
				else
				{
					if ((leaf->lsize - segsize) - (leaf->rsize + segsize) < 0)
						break;
				}

				 /* don't consider segments moved to right as unmodified */
				lastleftinfo->modified = true;
				leaf->lsize -= segsize;
				leaf->rsize += segsize;
				leaf->lastleft = dlist_prev_node(&leaf->segments, leaf->lastleft);
			}
		}
		Assert(leaf->lsize <= VodkaDataLeafMaxContentSize);
		Assert(leaf->rsize <= VodkaDataLeafMaxContentSize);

		/*
		 * Fetch the max item in the left page's last segment; it becomes the
		 * right bound of the page.
		 */
		lastleftinfo = dlist_container(leafSegmentInfo, node, leaf->lastleft);
		if (!lastleftinfo->items)
			lastleftinfo->items = vodkaPostingListDecode(lastleftinfo->seg,
													   &lastleftinfo->nitems);
		lbound = lastleftinfo->items[lastleftinfo->nitems - 1];

		*newlpage = MemoryContextAlloc(oldCxt, BLCKSZ);
		*newrpage = MemoryContextAlloc(oldCxt, BLCKSZ);

		dataPlaceToPageLeafSplit(buf, leaf, lbound, rbound,
								 prdata, *newlpage, *newrpage);

		Assert(VodkaPageRightMost(page) ||
			   vodkaCompareItemPointers(VodkaDataPageGetRightBound(*newlpage),
									  VodkaDataPageGetRightBound(*newrpage)) < 0);

		if (append)
			elog(DEBUG2, "appended %d items to block %u; split %d/%d (%d to go)",
				 maxitems, BufferGetBlockNumber(buf), (int) leaf->lsize, (int) leaf->rsize,
				 items->nitem - items->curitem - maxitems);
		else
			elog(DEBUG2, "inserted %d items to block %u; split %d/%d (%d to go)",
				 maxitems, BufferGetBlockNumber(buf), (int) leaf->lsize, (int) leaf->rsize,
				 items->nitem - items->curitem - maxitems);
	}

	MemoryContextSwitchTo(oldCxt);
	MemoryContextDelete(tmpCxt);

	items->curitem += maxitems;

	return needsplit ? SPLIT : INSERTED;
}

/*
 * Vacuum a posting tree leaf page.
 */
void
vodkaVacuumPostingTreeLeaf(Relation indexrel, Buffer buffer, VodkaVacuumState *gvs)
{
	Page		page = BufferGetPage(buffer);
	disassembledLeaf *leaf;
	bool		removedsomething = false;
	dlist_iter	iter;

	leaf = disassembleLeaf(page);

	/* Vacuum each segment. */
	dlist_foreach(iter, &leaf->segments)
	{
		leafSegmentInfo *sevodkafo = dlist_container(leafSegmentInfo, node, iter.cur);
		int			oldsegsize;
		ItemPointer cleaned;
		int			ncleaned;

		if (!sevodkafo->items)
			sevodkafo->items = vodkaPostingListDecode(sevodkafo->seg,
												  &sevodkafo->nitems);
		if (sevodkafo->seg)
			oldsegsize = SizeOfVodkaPostingList(sevodkafo->seg);
		else
			oldsegsize = VodkaDataLeafMaxContentSize;

		cleaned = vodkaVacuumItemPointers(gvs,
										sevodkafo->items,
										sevodkafo->nitems,
										&ncleaned);
		pfree(sevodkafo->items);
		sevodkafo->items = NULL;
		sevodkafo->nitems = 0;
		if (cleaned)
		{
			if (ncleaned > 0)
			{
				int			npacked;

				sevodkafo->seg = vodkaCompressPostingList(cleaned,
													  ncleaned,
													  oldsegsize,
													  &npacked);
				/* Removing an item never increases the size of the segment */
				if (npacked != ncleaned)
					elog(ERROR, "could not fit vacuumed posting list");
			}
			else
			{
				sevodkafo->seg = NULL;
				sevodkafo->items = NULL;
			}
			sevodkafo->nitems = ncleaned;
			sevodkafo->modified = true;

			removedsomething = true;
		}
	}

	/*
	 * If we removed any items, reconstruct the page from the pieces.
	 *
	 * We don't try to re-encode the segments here, even though some of them
	 * might be really small, now that we've removed some items from them.
	 * It seems like a waste of effort, as there isn't really any benefit from
	 * larger segments per se; larger segments only help you to pack more
	 * items in the same space. We might as well delay doing that until the
	 * next insertion, which will need to re-encode at least part of the page
	 * anyway.
	 *
	 * Also note if the page was in uncompressed, pre-9.4 format before, it
	 * is now represented as one huge segment that contains all the items.
	 * It might make sense to split that, to speed up random access, but we
	 * don't bother. You'll have to REINDEX anyway if you want the full gain
	 * of the new tighter index format.
	 */
	if (removedsomething)
	{
		XLogRecData *payloadrdata;

		START_CRIT_SECTION();
		dataPlaceToPageLeafRecompress(buffer, leaf, &payloadrdata);

		MarkBufferDirty(buffer);

		if (RelationNeedsWAL(indexrel))
		{
			XLogRecPtr	recptr;
			XLogRecData rdata;
			vodkaxlogVacuumDataLeafPage xlrec;

			xlrec.node = indexrel->rd_node;
			xlrec.blkno = BufferGetBlockNumber(buffer);

			rdata.buffer = InvalidBuffer;
			rdata.data = (char *) &xlrec;
			rdata.len = offsetof(vodkaxlogVacuumDataLeafPage, data);
			rdata.next = payloadrdata;

			recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_VACUUM_DATA_LEAF_PAGE, &rdata);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();
	}
}

/*
 * Assemble a disassembled posting tree leaf page back to a buffer.
 *
 * *prdata is filled with WAL information about this operation. The caller
 * is responsible for inserting to the WAL, along with any other information
 * about the operation that triggered this recompression.
 *
 * NOTE: The segment pointers can point directly to the same buffer, with
 * the limitation that any earlier segment must not overlap with an orivodkaal,
 * later segment. In other words, some segments may point the orivodkaal buffer
 * as long as you don't make any segments larger. Currently, leafRepackItems
 * satisies this rule because it rewrites all segments after the first
 * modified one, and vacuum can only make segments shorter.
 */
static void
dataPlaceToPageLeafRecompress(Buffer buf, disassembledLeaf *leaf,
							  XLogRecData **prdata)
{
	Page		page = BufferGetPage(buf);
	char	   *ptr;
	int			newsize;
	int			unmodifiedsize;
	bool		modified = false;
	dlist_iter	iter;
	/*
	 * these must be static so they can be returned to caller (no pallocs
	 * since we're in a critical section!)
	 */
	static vodkaxlogRecompressDataLeaf recompress_xlog;
	static XLogRecData rdata[2];

	ptr = (char *) VodkaDataLeafPageGetPostingList(page);
	newsize = 0;
	unmodifiedsize = 0;
	modified = false;
	dlist_foreach(iter, &leaf->segments)
	{
		leafSegmentInfo *sevodkafo = dlist_container(leafSegmentInfo, node, iter.cur);
		int			segsize;

		if (sevodkafo->modified)
			modified = true;

		/*
		 * Nothing to do with empty segments, except keep track if they've been
		 * modified
		 */
		if (sevodkafo->seg == NULL)
		{
			Assert(sevodkafo->items == NULL);
			continue;
		}

		segsize = SizeOfVodkaPostingList(sevodkafo->seg);

		if (!modified)
			unmodifiedsize += segsize;
		else
		{
			/*
			 * Use memmove rather than memcpy, in case the segment points
			 * to the same buffer
			 */
			memmove(ptr, sevodkafo->seg, segsize);
		}
		ptr += segsize;
		newsize += segsize;
	}
	Assert(newsize <= VodkaDataLeafMaxContentSize);
	VodkaDataLeafPageSetPostingListSize(page, newsize);

	/* Reset these in case the page was in pre-9.4 format before */
	VodkaPageSetCompressed(page);
	VodkaPageGetOpaque(page)->maxoff = InvalidOffsetNumber;

	/* Put WAL data */
	recompress_xlog.length = (uint16) newsize;
	recompress_xlog.unmodifiedsize = unmodifiedsize;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &recompress_xlog;
	rdata[0].len = offsetof(vodkaxlogRecompressDataLeaf, newdata);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = buf;
	rdata[1].buffer_std = TRUE;
	rdata[1].data = ((char *) VodkaDataLeafPageGetPostingList(page)) + unmodifiedsize;
	rdata[1].len = newsize - unmodifiedsize;
	rdata[1].next = NULL;

	*prdata = rdata;
}

/*
 * Like dataPlaceToPageLeafRecompress, but writes the disassembled leaf
 * segments to two pages instead of one.
 *
 * This is different from the non-split cases in that this does not modify
 * the orivodkaal page directly, but to temporary in-memory copies of the new
 * left and right pages.
 */
static void
dataPlaceToPageLeafSplit(Buffer buf, disassembledLeaf *leaf,
						 ItemPointerData lbound, ItemPointerData rbound,
						 XLogRecData **prdata, Page lpage, Page rpage)
{
	char	   *ptr;
	int			segsize;
	int			lsize;
	int			rsize;
	dlist_node *node;
	dlist_node *firstright;
	leafSegmentInfo *sevodkafo;
	/* these must be static so they can be returned to caller */
	static vodkaxlogSplitDataLeaf split_xlog;
	static XLogRecData rdata[3];

	/* Initialize temporary pages to hold the new left and right pages */
	VodkaInitPage(lpage, VODKA_DATA | VODKA_LEAF | VODKA_COMPRESSED, BLCKSZ);
	VodkaInitPage(rpage, VODKA_DATA | VODKA_LEAF | VODKA_COMPRESSED, BLCKSZ);

	/*
	 * Copy the segments that go to the left page.
	 *
	 * XXX: We should skip copying the unmodified part of the left page, like
	 * we do when recompressing.
	 */
	lsize = 0;
	ptr = (char *) VodkaDataLeafPageGetPostingList(lpage);
	firstright = dlist_next_node(&leaf->segments, leaf->lastleft);
	for (node = dlist_head_node(&leaf->segments);
		 node != firstright;
		 node = dlist_next_node(&leaf->segments, node))
	{
		sevodkafo = dlist_container(leafSegmentInfo, node, node);
		segsize = SizeOfVodkaPostingList(sevodkafo->seg);

		memcpy(ptr, sevodkafo->seg, segsize);
		ptr += segsize;
		lsize += segsize;
	}
	Assert(lsize == leaf->lsize);
	VodkaDataLeafPageSetPostingListSize(lpage, lsize);
	*VodkaDataPageGetRightBound(lpage) = lbound;

	/* Copy the segments that go to the right page */
	ptr = (char *) VodkaDataLeafPageGetPostingList(rpage);
	rsize = 0;
	for (node = firstright;
		 ;
		 node = dlist_next_node(&leaf->segments, node))
	{
		sevodkafo = dlist_container(leafSegmentInfo, node, node);
		segsize = SizeOfVodkaPostingList(sevodkafo->seg);

		memcpy(ptr, sevodkafo->seg, segsize);
		ptr += segsize;
		rsize += segsize;

		if (!dlist_has_next(&leaf->segments, node))
			break;
	}
	Assert(rsize == leaf->rsize);
	VodkaDataLeafPageSetPostingListSize(rpage, rsize);
	*VodkaDataPageGetRightBound(rpage) = rbound;

	/* Create WAL record */
	split_xlog.lsize = lsize;
	split_xlog.rsize = rsize;
	split_xlog.lrightbound = lbound;
	split_xlog.rrightbound = rbound;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &split_xlog;
	rdata[0].len = sizeof(vodkaxlogSplitDataLeaf);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = (char *) VodkaDataLeafPageGetPostingList(lpage);
	rdata[1].len = lsize;
	rdata[1].next = &rdata[2];

	rdata[2].buffer = InvalidBuffer;
	rdata[2].data = (char *) VodkaDataLeafPageGetPostingList(rpage);
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
static VodkaPlaceToPageRC
dataPlaceToPageInternal(VodkaBtree btree, Buffer buf, VodkaBtreeStack *stack,
						void *insertdata, BlockNumber updateblkno,
						XLogRecData **prdata, Page *newlpage, Page *newrpage)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber off = stack->off;
	PostingItem *pitem;
	/* these must be static so they can be returned to caller */
	static XLogRecData rdata;
	static vodkaxlogInsertDataInternal data;

	/* split if we have to */
	if (VodkaNonLeafDataPageGetFreeSpace(page) < sizeof(PostingItem))
	{
		dataSplitPageInternal(btree, buf, stack, insertdata, updateblkno,
							  prdata, newlpage, newrpage);
		return SPLIT;
	}

	*prdata = &rdata;
	Assert(VodkaPageIsData(page));

	START_CRIT_SECTION();

	/* Update existing downlink to point to next page (on internal page) */
	pitem = VodkaDataPageGetPostingItem(page, off);
	PostingItemSetBlockNumber(pitem, updateblkno);

	/* Add new item */
	pitem = (PostingItem *) insertdata;
	VodkaDataPageAddPostingItem(page, pitem, off);

	data.offset = off;
	data.newitem = *pitem;

	rdata.buffer = buf;
	rdata.buffer_std = false;
	rdata.data = (char *) &data;
	rdata.len = sizeof(vodkaxlogInsertDataInternal);
	rdata.next = NULL;

	return INSERTED;
}

/*
 * Places an item (or items) to a posting tree. Calls relevant function of
 * internal of leaf page because they are handled very differently.
 */
static VodkaPlaceToPageRC
dataPlaceToPage(VodkaBtree btree, Buffer buf, VodkaBtreeStack *stack,
				void *insertdata, BlockNumber updateblkno,
				XLogRecData **prdata,
				Page *newlpage, Page *newrpage)
{
	Page		page = BufferGetPage(buf);

	Assert(VodkaPageIsData(page));

	if (VodkaPageIsLeaf(page))
		return dataPlaceToPageLeaf(btree, buf, stack, insertdata,
								   prdata, newlpage, newrpage);
	else
		return dataPlaceToPageInternal(btree, buf, stack,
									   insertdata, updateblkno,
									   prdata, newlpage, newrpage);
}

/*
 * Split page and fill WAL record. Returns a new temp buffer filled with data
 * that should go to the left page. The orivodkaal buffer is left untouched.
 */
static void
dataSplitPageInternal(VodkaBtree btree, Buffer origbuf,
					  VodkaBtreeStack *stack,
					  void *insertdata, BlockNumber updateblkno,
					  XLogRecData **prdata, Page *newlpage, Page *newrpage)
{
	Page		oldpage = BufferGetPage(origbuf);
	OffsetNumber off = stack->off;
	int			nitems = VodkaPageGetOpaque(oldpage)->maxoff;
	Size		pageSize = PageGetPageSize(oldpage);
	ItemPointerData oldbound = *VodkaDataPageGetRightBound(oldpage);
	ItemPointer	bound;
	Page		lpage;
	Page		rpage;
	OffsetNumber separator;

	/* these must be static so they can be returned to caller */
	static vodkaxlogSplitDataInternal data;
	static XLogRecData rdata[4];
	static PostingItem allitems[(BLCKSZ / sizeof(PostingItem)) + 1];

	lpage = PageGetTempPage(oldpage);
	rpage = PageGetTempPage(oldpage);
	VodkaInitPage(lpage, VodkaPageGetOpaque(oldpage)->flags, pageSize);
	VodkaInitPage(rpage, VodkaPageGetOpaque(oldpage)->flags, pageSize);

	*prdata = rdata;

	/*
	 * First construct a new list of PostingItems, which includes all the
	 * old items, and the new item.
	 */
	memcpy(allitems, VodkaDataPageGetPostingItem(oldpage, FirstOffsetNumber),
		   (off - 1) * sizeof(PostingItem));

	allitems[off - 1] = *((PostingItem *) insertdata);
	memcpy(&allitems[off], VodkaDataPageGetPostingItem(oldpage, off),
		   (nitems - (off - 1)) * sizeof(PostingItem));
	nitems++;

	/* Update existing downlink to point to next page */
	PostingItemSetBlockNumber(&allitems[off], updateblkno);

	/*
	 * When creating a new index, fit as many tuples as possible on the left
	 * page, on the assumption that the table is scanned from bevodkaning to
	 * end. This packs the index as tight as possible.
	 */
	if (btree->isBuild && VodkaPageRightMost(oldpage))
		separator = VodkaNonLeafDataPageGetFreeSpace(rpage) / sizeof(PostingItem);
	else
		separator = nitems / 2;

	memcpy(VodkaDataPageGetPostingItem(lpage, FirstOffsetNumber), allitems, separator * sizeof(PostingItem));
	VodkaPageGetOpaque(lpage)->maxoff = separator;
	memcpy(VodkaDataPageGetPostingItem(rpage, FirstOffsetNumber),
		 &allitems[separator], (nitems - separator) * sizeof(PostingItem));
	VodkaPageGetOpaque(rpage)->maxoff = nitems - separator;

	/* set up right bound for left page */
	bound = VodkaDataPageGetRightBound(lpage);
	*bound = VodkaDataPageGetPostingItem(lpage,
								  VodkaPageGetOpaque(lpage)->maxoff)->key;

	/* set up right bound for right page */
	*VodkaDataPageGetRightBound(rpage) = oldbound;

	data.separator = separator;
	data.nitem = nitems;
	data.rightbound = oldbound;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(vodkaxlogSplitDataInternal);
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
dataPrepareDownlink(VodkaBtree btree, Buffer lbuf)
{
	PostingItem *pitem = palloc(sizeof(PostingItem));
	Page		lpage = BufferGetPage(lbuf);

	PostingItemSetBlockNumber(pitem, BufferGetBlockNumber(lbuf));
	pitem->key = *VodkaDataPageGetRightBound(lpage);

	return pitem;
}

/*
 * Fills new root by right bound values from child.
 * Also called from vodkaxlog, should not use btree
 */
void
vodkaDataFillRoot(VodkaBtree btree, Page root, BlockNumber lblkno, Page lpage, BlockNumber rblkno, Page rpage)
{
	PostingItem li,
				ri;

	li.key = *VodkaDataPageGetRightBound(lpage);
	PostingItemSetBlockNumber(&li, lblkno);
	VodkaDataPageAddPostingItem(root, &li, InvalidOffsetNumber);

	ri.key = *VodkaDataPageGetRightBound(rpage);
	PostingItemSetBlockNumber(&ri, rblkno);
	VodkaDataPageAddPostingItem(root, &ri, InvalidOffsetNumber);
}


/*** Functions to work with disassembled leaf pages ***/

/*
 * Disassemble page into a disassembledLeaf struct.
 */
static disassembledLeaf *
disassembleLeaf(Page page)
{
	disassembledLeaf *leaf;
	VodkaPostingList *seg;
	Pointer		segbegin;
	Pointer		segend;

	leaf = palloc0(sizeof(disassembledLeaf));
	dlist_init(&leaf->segments);

	if (VodkaPageIsCompressed(page))
	{
		/*
		 * Create a leafSegment entry for each segment.
		 */
		seg = VodkaDataLeafPageGetPostingList(page);
		segbegin = (Pointer) seg;
		segend = segbegin + VodkaDataLeafPageGetPostingListSize(page);
		while ((Pointer) seg < segend)
		{
			leafSegmentInfo *sevodkafo = palloc(sizeof(leafSegmentInfo));

			sevodkafo->seg = seg;
			sevodkafo->items = NULL;
			sevodkafo->nitems = 0;
			sevodkafo->modified = false;
			dlist_push_tail(&leaf->segments, &sevodkafo->node);

			seg = VodkaNextPostingListSegment(seg);
		}
	}
	else
	{
		/*
		 * A pre-9.4 format uncompressed page is represented by a single
		 * segment, with an array of items.
		 */
		ItemPointer uncompressed;
		int			nuncompressed;
		leafSegmentInfo *sevodkafo;

		uncompressed = dataLeafPageGetUncompressed(page, &nuncompressed);

		sevodkafo = palloc(sizeof(leafSegmentInfo));

		sevodkafo->seg = NULL;
		sevodkafo->items = palloc(nuncompressed * sizeof(ItemPointerData));
		memcpy(sevodkafo->items, uncompressed, nuncompressed * sizeof(ItemPointerData));
		sevodkafo->nitems = nuncompressed;
		/* make sure we rewrite this to disk */
		sevodkafo->modified = true;

		dlist_push_tail(&leaf->segments, &sevodkafo->node);
	}

	return leaf;
}

/*
 * Distribute newItems to the segments.
 *
 * Any segments that acquire new items are decoded, and the new items are
 * merged with the old items.
 *
 * Returns true if any new items were added. False means they were all
 * duplicates of existing items on the page.
 */
static bool
addItemsToLeaf(disassembledLeaf *leaf, ItemPointer newItems, int nNewItems)
{
	dlist_iter	iter;
	ItemPointer nextnew = newItems;
	int			newleft = nNewItems;
	bool		modified = false;

	/*
	 * If the page is completely empty, just construct one new segment to
	 * hold all the new items.
	 */
	if (dlist_is_empty(&leaf->segments))
	{
		/* create a new segment for the new entries */
		leafSegmentInfo *sevodkafo = palloc(sizeof(leafSegmentInfo));

		sevodkafo->seg = NULL;
		sevodkafo->items = newItems;
		sevodkafo->nitems = nNewItems;
		sevodkafo->modified = true;
		dlist_push_tail(&leaf->segments, &sevodkafo->node);
		return true;
	}

	dlist_foreach(iter, &leaf->segments)
	{
		leafSegmentInfo *cur = (leafSegmentInfo *)  dlist_container(leafSegmentInfo, node, iter.cur);
		int			nthis;
		ItemPointer	tmpitems;
		int			ntmpitems;

		/*
		 * How many of the new items fall into this segment?
		 */
		if (!dlist_has_next(&leaf->segments, iter.cur))
			nthis = newleft;
		else
		{
			leafSegmentInfo *next;
			ItemPointerData next_first;

			next = (leafSegmentInfo *) dlist_container(leafSegmentInfo, node,
								   dlist_next_node(&leaf->segments, iter.cur));
			if (next->items)
				next_first = next->items[0];
			else
			{
				Assert(next->seg != NULL);
				next_first = next->seg->first;
			}

			nthis = 0;
			while (nthis < newleft && vodkaCompareItemPointers(&nextnew[nthis], &next_first) < 0)
				nthis++;
		}
		if (nthis == 0)
			continue;

		/* Merge the new items with the existing items. */
		if (!cur->items)
			cur->items = vodkaPostingListDecode(cur->seg, &cur->nitems);

		tmpitems = palloc((cur->nitems + nthis) * sizeof(ItemPointerData));
		ntmpitems = vodkaMergeItemPointers(tmpitems,
										 cur->items, cur->nitems,
										 nextnew, nthis);
		if (ntmpitems != cur->nitems)
		{
			cur->items = tmpitems;
			cur->nitems = ntmpitems;
			cur->seg = NULL;
			modified = cur->modified = true;
		}

		nextnew += nthis;
		newleft -= nthis;
		if (newleft == 0)
			break;
	}

	return modified;
}

/*
 * Recompresses all segments that have been modified.
 *
 * If not all the items fit on two pages (ie. after split), we store as
 * many items as fit, and set *remaining to the first item that didn't fit.
 * If all items fit, *remaining is set to invalid.
 *
 * Returns true if the page has to be split.
 *
 * XXX: Actually, this re-encodes all segments after the first one that was
 * modified, to make sure the new segments are all more or less of equal
 * length. That's unnecessarily aggressive; if we've only added a single item
 * to one segment, for example, we could re-encode just that single segment,
 * as long as it's still smaller than, say, 2x the normal segment size.
 */
static bool
leafRepackItems(disassembledLeaf *leaf, ItemPointer remaining)
{
	dlist_iter	iter;
	ItemPointer allitems;
	int			nallitems;
	int			pgused = 0;
	bool		needsplit;
	int			totalpacked;
	dlist_mutable_iter miter;
	dlist_head	recode_list;
	int			nrecode;
	bool		recoding;

	ItemPointerSetInvalid(remaining);

	/*
	 * Find the first segment that needs to be re-coded. Move all segments
	 * that need recoding to separate list, and count the total number of
	 * items in them. Also, add up the number of bytes in unmodified segments
	 * (pgused).
	 */
	dlist_init(&recode_list);
	recoding = false;
	nrecode = 0;
	pgused = 0;
	dlist_foreach_modify(miter, &leaf->segments)
	{
		leafSegmentInfo *sevodkafo = dlist_container(leafSegmentInfo, node, miter.cur);

		/* If the segment was modified, re-encode it */
		if (sevodkafo->modified || sevodkafo->seg == NULL)
			recoding = true;
		/*
		 * Also re-encode abnormally small or large segments. (Vacuum can
		 * leave behind small segments, and conversion from pre-9.4 format
		 * can leave behind large segments).
		 */
		else if (SizeOfVodkaPostingList(sevodkafo->seg) < VodkaPostingListSegmentMinSize)
			recoding = true;
		else if (SizeOfVodkaPostingList(sevodkafo->seg) > VodkaPostingListSegmentMaxSize)
			recoding = true;

		if (recoding)
		{
			if (!sevodkafo->items)
				sevodkafo->items = vodkaPostingListDecode(sevodkafo->seg,
													  &sevodkafo->nitems);
			nrecode += sevodkafo->nitems;

			dlist_delete(miter.cur);
			dlist_push_tail(&recode_list, &sevodkafo->node);
		}
		else
			pgused += SizeOfVodkaPostingList(sevodkafo->seg);
	}

	if (nrecode == 0)
		return false; /* nothing to do */

	/*
	 * Construct one big array of the items that need to be re-encoded.
	 */
	allitems = palloc(nrecode * sizeof(ItemPointerData));
	nallitems = 0;
	dlist_foreach(iter, &recode_list)
	{
		leafSegmentInfo *sevodkafo = dlist_container(leafSegmentInfo, node, iter.cur);
		memcpy(&allitems[nallitems], sevodkafo->items, sevodkafo->nitems * sizeof(ItemPointerData));
		nallitems += sevodkafo->nitems;
	}
	Assert(nallitems == nrecode);

	/*
	 * Start packing the items into segments. Stop when we have consumed
	 * enough space to fill both pages, or we run out of items.
	 */
	totalpacked = 0;
	needsplit = false;
	while (totalpacked < nallitems)
	{
		leafSegmentInfo *sevodkafo;
		int			npacked;
		VodkaPostingList *seg;
		int			segsize;

		seg = vodkaCompressPostingList(&allitems[totalpacked],
									 nallitems - totalpacked,
									 VodkaPostingListSegmentMaxSize,
									 &npacked);
		segsize = SizeOfVodkaPostingList(seg);
		if (pgused + segsize > VodkaDataLeafMaxContentSize)
		{
			if (!needsplit)
			{
				/* switch to right page */
				Assert(pgused > 0);
				leaf->lastleft = dlist_tail_node(&leaf->segments);
				needsplit = true;
				leaf->lsize = pgused;
				pgused = 0;
			}
			else
			{
				/* filled both pages */
				*remaining = allitems[totalpacked];
				break;
			}
		}

		sevodkafo = palloc(sizeof(leafSegmentInfo));
		sevodkafo->seg = seg;
		sevodkafo->items = &allitems[totalpacked];
		sevodkafo->nitems = npacked;
		sevodkafo->modified = true;

		dlist_push_tail(&leaf->segments, &sevodkafo->node);

		pgused += segsize;
		totalpacked += npacked;
	}

	if (!needsplit)
	{
		leaf->lsize = pgused;
		leaf->rsize = 0;
	}
	else
		leaf->rsize = pgused;

	Assert(leaf->lsize <= VodkaDataLeafMaxContentSize);
	Assert(leaf->rsize <= VodkaDataLeafMaxContentSize);

	return needsplit;
}


/*** Functions that are exported to the rest of the VODKA code ***/

/*
 * Creates new posting tree containing the given TIDs. Returns the page
 * number of the root of the new posting tree.
 *
 * items[] must be in sorted order with no duplicates.
 */
BlockNumber
vodkaCreatePostingTree(Relation index, ItemPointerData *items, uint32 nitems,
				  VodkaStatsData *buildStats)
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
	buffer = VodkaNewBuffer(index);
	page = BufferGetPage(buffer);
	blkno = BufferGetBlockNumber(buffer);

	START_CRIT_SECTION();

	VodkaInitPage(page, VODKA_DATA | VODKA_LEAF | VODKA_COMPRESSED, BLCKSZ);
	VodkaPageGetOpaque(page)->rightlink = InvalidBlockNumber;

	/*
	 * Write as many of the items to the root page as fit. In segments
	 * of max VodkaPostingListSegmentMaxSize bytes each.
	 */
	nrootitems = 0;
	rootsize = 0;
	ptr = (Pointer) VodkaDataLeafPageGetPostingList(page);
	while (nrootitems < nitems)
	{
		VodkaPostingList *segment;
		int			npacked;
		int			segsize;

		segment = vodkaCompressPostingList(&items[nrootitems],
										 nitems - nrootitems,
										 VodkaPostingListSegmentMaxSize,
										 &npacked);
		segsize = SizeOfVodkaPostingList(segment);
		if (rootsize + segsize > VodkaDataLeafMaxContentSize)
			break;

		memcpy(ptr, segment, segsize);
		ptr += segsize;
		rootsize += segsize;
		nrootitems += npacked;
		pfree(segment);
	}
	VodkaDataLeafPageSetPostingListSize(page, rootsize);
	MarkBufferDirty(buffer);

	elog(DEBUG2, "created VODKA posting tree with %d items", nrootitems);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		vodkaxlogCreatePostingTree data;

		data.node = index->rd_node;
		data.blkno = blkno;
		data.size = rootsize;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &data;
		rdata[0].len = sizeof(vodkaxlogCreatePostingTree);
		rdata[0].next = &rdata[1];

		rdata[1].buffer = InvalidBuffer;
		rdata[1].data = (char *) VodkaDataLeafPageGetPostingList(page);
		rdata[1].len = rootsize;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_CREATE_PTREE, rdata);
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
		vodkaInsertItemPointers(index, blkno,
							  items + nrootitems,
							  nitems - nrootitems,
							  buildStats);
	}

	return blkno;
}

void
vodkaPrepareDataScan(VodkaBtree btree, Relation index, BlockNumber rootBlkno)
{
	memset(btree, 0, sizeof(VodkaBtreeData));

	btree->index = index;
	btree->rootBlkno = rootBlkno;

	btree->findChildPage = dataLocateItem;
	btree->getLeftMostChild = dataGetLeftMostPage;
	btree->isMoveRight = dataIsMoveRight;
	btree->findItem = NULL;
	btree->findChildPtr = dataFindChildPtr;
	btree->placeToPage = dataPlaceToPage;
	btree->fillRoot = vodkaDataFillRoot;
	btree->prepareDownlink = dataPrepareDownlink;

	btree->isData = TRUE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;
}

/*
 * Inserts array of item pointers, may execute several tree scan (very rare)
 */
void
vodkaInsertItemPointers(Relation index, BlockNumber rootBlkno,
					  ItemPointerData *items, uint32 nitem,
					  VodkaStatsData *buildStats)
{
	VodkaBtreeData btree;
	VodkaBtreeDataLeafInsertData insertdata;
	VodkaBtreeStack *stack;

	vodkaPrepareDataScan(&btree, index, rootBlkno);
	btree.isBuild = (buildStats != NULL);
	insertdata.items = items;
	insertdata.nitem = nitem;
	insertdata.curitem = 0;

	while (insertdata.curitem < insertdata.nitem)
	{
		/* search for the leaf page where the first item should go to */
		btree.itemptr = insertdata.items[insertdata.curitem];
		stack = vodkaFindLeafPage(&btree, false);

		vodkaInsertValue(&btree, stack, &insertdata, buildStats);
	}
}

/*
 * Starts a new scan on a posting tree.
 */
VodkaBtreeStack *
vodkaScanBevodkaPostingTree(VodkaBtree btree, Relation index, BlockNumber rootBlkno)
{
	VodkaBtreeStack *stack;

	vodkaPrepareDataScan(btree, index, rootBlkno);

	btree->fullScan = TRUE;

	stack = vodkaFindLeafPage(btree, TRUE);

	return stack;
}
