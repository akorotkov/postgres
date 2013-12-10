/*-------------------------------------------------------------------------
 *
 * gindatapage.c
 *	  routines for handling GIN posting tree pages.
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
#include "access/heapam_xlog.h"
#include "miscadmin.h"
#include "utils/rel.h"

/*
 * Initialize leaf page of posting tree. Reserves space for item indexes at
 * the end of page.
 */
static void
GinInitDataLeafPage(Page page)
{
	GinInitPage(page, GIN_DATA | GIN_LEAF | GIN_COMPRESSED, BLCKSZ);
	((PageHeader) page)->pd_upper -= sizeof(GinDataLeafItemIndex) * GinDataLeafIndexCount;
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
findInLeafPageCompressed(GinBtree btree, Page page,
						 ItemPointerData *iptrOut, Pointer *ptrOut)
{
	int			i;
	ItemPointerData iptr = {{0,0},0};
	uint64		state = 0;
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
		if (index->offset == 0)
			break;

		Assert(index->offset < GinDataLeafPageGetPostingListSize(page));

		cmp = ginCompareItemPointers(&index->iptr, &btree->itemptr);
		if (cmp < 0)
		{
			ptr = GinDataLeafPageGetPostingList(page) + index->offset;
			iptr = index->iptr;
		}
		else
		{
			endPtr = GinDataLeafPageGetPostingList(page) + index->offset;
			break;
		}
	}
	Assert(ptr <= endPtr);

	/* Search page in [first, maxoff] range found by page index */
	state = ginPostingListDecodeBegin(&iptr);
	while (ptr < endPtr)
	{
		Pointer prev_ptr = ptr;
		ItemPointerData prev_iptr = iptr;

		ptr = ginPostingListDecode(ptr, &iptr, &state);

		Assert(ptr <= endPtr);

		cmp = ginCompareItemPointers(&btree->itemptr, &iptr);
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

	*ptrOut = ptr;
	*iptrOut = iptr;
	return result;
}

/*
 * Find correct PostingItem in an uncompressed (ie. pre-9.4) non-leaf page.
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

		result = ginCompareItemPointers(&btree->itemptr,
										GinDataPageGetItemPointer(page, mid));

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
		stack->off = GinDataLeafPageGetPostingList(page) - page;
		return TRUE;
	}

	if (GinPageIsCompressed(page))
	{
		result = findInLeafPageCompressed(btree, page, &stack->iptr, &ptr);
		stack->off = ptr - page;
	}
	else
	{
		result = findInLeafPageUncompressed(btree, page, &stack->off);
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
 * Convert all items on an uncompressed posting tree leaf page into compressed
 * format. The result is palloc'd, and the size of the compressed posting list
 * is stored in *tmpSz.
 *
 * Note that in some rare cases the compressed posting list can take more space
 * than the uncompressed one; compressing a 4-byte block number can take up to
 * 5 bytes in encoded form. The compressed posting list might be too large to
 * even fit on a single compressed page, particularly because the item indexes
 * take up some space in compressed pages. The callers need to beware of that.
 */
static CompressedPostingList
dataCompressLeafPage(Page page, Size *tmpSz)
{
  OffsetNumber  maxoff;
  CompressedPostingList ptr;

  maxoff = GinPageGetOpaque(page)->maxoff;

  ptr = palloc(GinDataLeafMaxPostingListSize * 2);

  /* Compress TIDs of the page into temporary buffer */
  if (!ginCompressPostingList(GinDataPageGetItemPointer(page, FirstOffsetNumber),
                maxoff + 1, GinDataLeafMaxPostingListSize * 2,
                ptr, tmpSz))
  {
    /*
     * Oops, won't fit. Shouldn't happen, at least not with a sane page
     * size, but better safe than sorry.
     */
    elog(ERROR, "compressed gin leaf page doesn't fit even after split");
  }
  return ptr;
}

/*
 * Places keys to leaf data page and fills WAL record.
 */
static bool
dataPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack,
          void *insertdata, XLogRecData **prdata)
{
  GinBtreeDataLeafInsertData *items = insertdata;
  ItemPointer newItems = &items->items[items->curitem];
  int     maxitems = items->nitem - items->curitem;
  Page    page = BufferGetPage(buf);
  char     *endPtr;
  Pointer   ptr, insertBegin, insertEnd;
  ItemPointerData nextIptr;
  char    nextpacked[MAX_COMPRESSED_ITEM_POINTER_SIZE];
  Pointer   restPtr;
  int     newlen;
  int     nextlen;
  int     restlen;
  int     freespace;
  int     i;
  ItemPointerData rbound;
  int     oldsize;
  int     newsize;
  char    newCompressedItems[BLCKSZ];
  uint64    writestate;

  /* these must be static so they can be returned to caller */
  static XLogRecData rdata[2];
  static ginxlogInsertDataLeaf data;

  Assert(GinPageIsData(page));

  /* Try to compress the page if it's uncompressed */
  if (!GinPageIsCompressed(page))
  {
    Size    tmpSz;
    char     *tmpBuf;
    BlockNumber rightlink;
    ItemPointerData rbound;

    tmpBuf = dataCompressLeafPage(page, &tmpSz);

    elog(DEBUG1, "compressed page %u from %d to %d bytes",
       BufferGetBlockNumber(buf),
       (int) (GinPageGetOpaque(page)->maxoff * sizeof(ItemPointerData)),
       (int) tmpSz);

    /*
     * There must be enough room after compression for the new item.
     * If not, we'll have to split the page anyway, so don't do anything
     * now, and let the split take care of rewriting the split pages as
     * compressed.
     */
    if (tmpSz + MAX_COMPRESSED_ITEM_POINTER_SIZE > GinDataLeafMaxPostingListSize)
    {
      pfree(tmpBuf);
      return false;
    }

    /* Reinitialize page as compressed */
    START_CRIT_SECTION();
    rightlink = GinPageGetOpaque(page)->rightlink;
    rbound = *GinDataPageGetRightBound(page);

    GinInitDataLeafPage(page);
    GinPageGetOpaque(page)->rightlink = rightlink;
    *GinDataPageGetRightBound(page) = rbound;

    memcpy(GinDataLeafPageGetPostingList(page), tmpBuf, tmpSz);
    GinDataLeafPageSetPostingListSize(page, tmpSz);
    ginRebuildItemIndexes(page);

    MarkBufferDirty(buf);
    log_newpage_buffer(buf, true);
    END_CRIT_SECTION();

    pfree(tmpBuf);

    /*
     * Ok, the page is now compressed. Proceed to insert the new item to
     * it as usual.
     */
    findInLeafPageCompressed(btree, page, &stack->iptr, &ptr);
    stack->off = ptr - page;
  }

  endPtr = GinDataLeafPageGetPostingListEnd(page);

  /*
   * stack->pageOffset points to the location where we're going to insert
   * the new item(s), and stack->iptr is the corresponding item pointer
   * at that location.
   */
  oldsize = GinDataLeafPageGetPostingListSize(page);
  insertBegin = page + stack->off;
  freespace = GinDataLeafPageGetFreeSpace(page);

  Assert(insertBegin <= endPtr && insertBegin >= GinDataLeafPageGetPostingList(page));

  /*
   * Read the next item pointer. This will become the first item pointer
   * after the ones we insert.
   */
  restPtr = insertBegin;
  if (insertBegin < endPtr)
  {
    uint64  state = ginPostingListDecodeBegin(&stack->iptr);
    restPtr = ginPostingListDecode(insertBegin, &nextIptr, &state);
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
   * Encode as many of the new items as fit on this page.
   *
   * Note: The size of the first item after the inserted one might change
   * when it's re-encoded. But it can never become larger, because the
   * distance from the previous item gets smaller, not larger. We ignore
   * that effect, and possibly waste a few bytes.
   */
  writestate = ginPostingListEncodeBegin(&stack->iptr);
  freespace = GinDataLeafPageGetFreeSpace(page);
  ptr = newCompressedItems;
  for (i = 0; i < maxitems; i++)
  {
    Pointer newptr;

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

    newptr = ginPostingListEncode(ptr, &newItems[i], &writestate);
    if (newptr - newCompressedItems > freespace)
    {
      /*
       * This doesn't fit anymore. We could insert what fits, but it's
       * better to give up and proceed to split the page immediately.
       * We'd have to split to insert the next item anyway, so might
       * as well save the effort of writing these items to the page
       * first.
       */
      return false;
    }
    ptr = newptr;
  }
  newlen = ptr - newCompressedItems;
  maxitems = i;
  if (maxitems == 0)
    return false; /* no entries fit on this page */

  /*
   * We have to re-encode the first old item pointer after the ones we're
   * inserting. Encode it in a small temporary buffer, so that we know
   * how much space it will take re-encoded.
   */
  if (insertBegin < endPtr)
    nextlen = ginPostingListEncode(nextpacked, &nextIptr, &writestate) - nextpacked;

  Assert(GinDataPageFreeSpacePre(page, insertBegin + newlen + nextlen + restlen) >= 0);

  /*
   * Ok, we have collected all the information we need. Shift any entries
   * after the insertion point that we are not going to otherwise modify.
   * Once we start modifying the page, there's no turning back. The caller
   * is responsible for calling END_CRIT_SECTION() after writing the WAL
   * record.
   */
  START_CRIT_SECTION();
  insertEnd = insertBegin + newlen + nextlen;
  if (restlen > 0)
    memmove(insertEnd, restPtr, restlen);

  /* Write the new items */
  memcpy(insertBegin, newCompressedItems, newlen);
  ptr = insertBegin + newlen;

  /* Write the re-encoded old item */
  if (nextlen > 0)
  {
    memcpy(ptr, nextpacked, nextlen);
    ptr += nextlen;
    Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);
  }
  Assert(ptr == insertEnd);

  newsize = insertEnd + restlen - GinDataLeafPageGetPostingList(page);
  Assert(newsize <= GinDataLeafMaxPostingListSize);
  GinDataLeafPageSetPostingListSize(page, newsize);

  /* Update the page item indexes */
  ginUpdateItemIndexes(page, restPtr - GinDataLeafPageGetPostingList(page), newsize - oldsize);

  /* Put WAL data */
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

  *prdata = rdata;

  items->curitem += maxitems;

  return true;
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
            XLogRecData **prdata)
{
  Page    page = BufferGetPage(buf);
  OffsetNumber off = stack->off;
  PostingItem *pitem;
  /* these must be static so they can be returned to caller */
  static XLogRecData rdata;
  static ginxlogInsertDataInternal data;

  /* quick exit if it doesn't fit */
  if (GinNonLeafDataPageGetFreeSpace(page) < sizeof(PostingItem))
    return false;

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
  rdata.buffer = InvalidBuffer;
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
        XLogRecData **prdata)
{
  Page    page = BufferGetPage(buf);

  Assert(GinPageIsData(page));

  if (GinPageIsLeaf(page))
    return dataPlaceToPageLeaf(btree, buf, stack, insertdata, prdata);
  else
    return dataPlaceToPageInternal(btree, buf, stack,
                     insertdata, updateblkno, prdata);
}

/*
 * Place tuple and split page, original buffer(lbuf) is left untouched,
 * returns shadow page of lbuf filled new data.
 */
static Page
dataSplitPageLeaf(GinBtree btree, Buffer lbuf, Buffer rbuf,
          GinBtreeStack *stack,
          void *insertdata, BlockNumber updateblkno,
          XLogRecData **prdata)
{
  GinBtreeDataLeafInsertData *items = insertdata;
  ItemPointer newItems = &items->items[items->curitem];
  int     maxitems = items->nitem - items->curitem;
  int     curitem;
  Size    totalsize;
  Pointer   ptr;
  Pointer   oldPtr, oldEndPtr;
  Page    oldPage = BufferGetPage(lbuf);
  Page    page;
  Page    lpage = PageGetTempPage(oldPage);
  Page    rpage = BufferGetPage(rbuf);
  ItemPointerData iptr, maxLeftIptr;
  ItemPointerData nextold, nextnew;
  uint64    oldreadstate = 0;
  uint64    writestate;
  int     i;
  int     splitpoint;
  int     freespace;
  ItemPointerData rbound;

  /* these must be static so they can be returned to caller */
  static ginxlogSplitDataLeaf data;
  static XLogRecData rdata[3];

  *prdata = rdata;

  /*
   * If the page was uncompressed, compress all the old items into a
   * temporary area first. We'll read the items from the temporary area as
   * if it was the old page.
   */
  if (!GinPageIsCompressed(oldPage))
  {
    oldPtr = dataCompressLeafPage(oldPage, &totalsize);
    oldEndPtr = oldPtr + totalsize;

    elog(LOG, "compressed page %u from %d to %d bytes (SPLIT)",
       BufferGetBlockNumber(lbuf),
       (int) (GinPageGetOpaque(oldPage)->maxoff * sizeof(ItemPointerData)),
       (int) totalsize);
  }
  else
  {
    oldPtr = GinDataLeafPageGetPostingList(oldPage);
    oldEndPtr = GinDataLeafPageGetPostingListEnd(oldPage);
    totalsize = oldEndPtr - oldPtr;
  }

  /*
   * Estimate the total size of the posting lists we're going to store on
   * the two halves.
   *
   * Try to place as many items as we can if we're placing them to the end
   * of rightmost page. Otherwise place one item. We'll reserve 2x
   * MAX_COMPRESSED_ITEM_POINTER_SIZE. One because possible unevenness of
   * split and another because of re-encoding first item pointer of right
   * page from zero.
   */
  if (GinPageRightMost(oldPage))
    ItemPointerSetMax(&rbound);
  else
  {
    /*
     * The right bound stored on page is inclusive, but for the comparison
     * below, we want an exclusive value
     */
    rbound = *GinDataPageGetRightBound(oldPage);
    rbound.ip_posid++;
  }
  freespace = 2 * GinDataLeafMaxPostingListSize - 2 * MAX_COMPRESSED_ITEM_POINTER_SIZE;
  MemSet(&iptr, 0, sizeof(ItemPointerData));
  for (i = 0; i < maxitems; i++)
  {
    int sz;

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

    sz = ginPostingListPackedSize(&newItems[i], &iptr);
    if (totalsize + sz >= freespace)
      break;    /* doesn't fit */
    iptr = newItems[i];
    totalsize += sz;
  }
  maxitems = i;
  Assert(totalsize < freespace);
  Assert(maxitems > 0);

  /* Reinitialize pages */
  GinInitDataLeafPage(lpage);
  GinInitDataLeafPage(rpage);
  GinDataLeafPageSetPostingListSize(lpage, 0);
  GinDataLeafPageSetPostingListSize(rpage, 0);

  /*
   * When creating a new index, fit as many items as possible on the left
   * page, on the assumption that the table is scanned from beginning to
   * end. This packs the index as tight as possible.
   */
  if (btree->isBuild && GinPageRightMost(oldPage) &&
    totalsize > GinDataLeafPageGetFreeSpace(lpage) - MAX_COMPRESSED_ITEM_POINTER_SIZE)
    splitpoint = GinDataLeafPageGetFreeSpace(lpage) - MAX_COMPRESSED_ITEM_POINTER_SIZE;
  else
    splitpoint = totalsize / 2;

  /*
   * Copy the old and new items to the new pages.
   */
  if (oldPtr < oldEndPtr)
  {
    oldreadstate = 0;
    oldPtr = ginPostingListDecode(oldPtr, &nextold, &oldreadstate);
  }
  else
    ItemPointerSetInvalid(&nextold);

  nextnew = newItems[0];
  curitem = 1;

  writestate = 0;
  ptr = GinDataLeafPageGetPostingList(lpage);
  page = lpage;
  while (ItemPointerIsValid(&nextnew) || ItemPointerIsValid(&nextold))
  {
    ItemPointerData item;
    int     cmp;

    /*
     * Pull the next item from the old page, or from the array of new items.
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
        oldPtr = ginPostingListDecode(oldPtr, &nextold, &oldreadstate);
      else
        ItemPointerSetInvalid(&nextold);
    }
    else
    {
      item = nextnew;
      if (curitem < maxitems)
      {
        nextnew = newItems[curitem];
        curitem++;
      }
      else
        ItemPointerSetInvalid(&nextnew);
    }

    /* Write the tuple to the current half. */
    ptr = ginPostingListEncode(ptr, &item, &writestate);
    Assert(GinDataPageFreeSpacePre(page, ptr) >= 0);

    /* Check if it's time to switch to the right page */
    if (ptr - GinDataLeafPageGetPostingList(page) > splitpoint &&
      page == lpage)
    {
      maxLeftIptr = item;
      GinDataLeafPageSetPostingListSize(lpage, ptr - GinDataLeafPageGetPostingList(lpage));
      writestate = 0;
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
  ginRebuildItemIndexes(lpage);
  ginRebuildItemIndexes(rpage);

  data.separator = GinDataLeafPageGetPostingListSize(lpage);
  data.nbytes = GinDataLeafPageGetPostingListSize(lpage) + GinDataLeafPageGetPostingListSize(rpage);
  data.lrightbound = *GinDataPageGetRightBound(lpage);

  rdata[0].buffer = InvalidBuffer;
  rdata[0].data = (char *) &data;
  rdata[0].len = sizeof(ginxlogSplitDataLeaf);
  rdata[0].next = &rdata[1];

  rdata[1].buffer = InvalidBuffer;
  rdata[1].data = GinDataLeafPageGetPostingList(lpage);
  rdata[1].len = GinDataLeafPageGetPostingListSize(lpage);
  rdata[1].next = &rdata[2];

  rdata[2].buffer = InvalidBuffer;
  rdata[2].data = GinDataLeafPageGetPostingList(rpage);
  rdata[2].len = GinDataLeafPageGetPostingListSize(rpage);
  rdata[2].next = NULL;

  items->curitem += maxitems;

  return lpage;
}

/*
 * Split page and fill WAL record. Returns a new temp buffer filled with data
 * that should go to the left page. The original buffer (lbuf) is left
 * untouched.
 */
static Page
dataSplitPageInternal(GinBtree btree, Buffer lbuf, Buffer rbuf,
            GinBtreeStack *stack,
            void *insertdata, BlockNumber updateblkno,
            XLogRecData **prdata)
{
  Page    oldpage = BufferGetPage(lbuf);
  OffsetNumber off = stack->off;
  int     nitems = GinPageGetOpaque(oldpage)->maxoff;
  Size    pageSize = PageGetPageSize(oldpage);
  ItemPointerData oldbound = *GinDataPageGetRightBound(oldpage);
  ItemPointer bound;
  Page    lpage;
  Page    rpage;
  OffsetNumber separator;

  /* these must be static so they can be returned to caller */
  static ginxlogSplitDataInternal data;
  static XLogRecData rdata[4];
  static PostingItem allitems[(BLCKSZ / sizeof(PostingItem)) + 1];

  lpage = PageGetTempPage(oldpage);
  rpage = BufferGetPage(rbuf);
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

  rdata[0].buffer = InvalidBuffer;
  rdata[0].data = (char *) &data;
  rdata[0].len = sizeof(ginxlogSplitDataInternal);
  rdata[0].next = &rdata[1];

  rdata[1].buffer = InvalidBuffer;
  rdata[1].data = (char *) allitems;
  rdata[1].len = nitems * sizeof(PostingItem);
  rdata[1].next = NULL;

  return lpage;
}

/*
 * Split page of posting tree. Calls relevant function of internal of leaf page
 * because they are handled very differently.
 */
static Page
dataSplitPage(GinBtree btree, Buffer lbuf, Buffer rbuf,
        GinBtreeStack *stack,
        void *insertdata, BlockNumber updateblkno,
        XLogRecData **prdata)
{
  if (GinPageIsLeaf(BufferGetPage(lbuf)))
    return dataSplitPageLeaf(btree, lbuf, rbuf, stack,
                 insertdata, updateblkno, prdata);
  else
    return dataSplitPageInternal(btree, lbuf, rbuf, stack,
                   insertdata, updateblkno, prdata);
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


/*
 * We try to keep the item indexes balanced so that each "bin" between two
 * indexes has roughly the same size.
 */
#define TARGET_BIN_SIZE GinDataLeafMaxPostingListSize / (GinDataLeafIndexCount + 1)

/*
 * Recreate the item indexes in the end of leaf page, which are used to
 * speed up random access into the page.
 */
void
ginRebuildItemIndexes(Page page)
{
	GinDataLeafItemIndex *indexes = GinPageGetIndexes(page);
	Pointer		beginptr;
	Pointer		ptr;
	Pointer		endptr;
	Pointer		nextptr;
	ItemPointerData iptr;
	int			i, j;
	uint64		readstate;

	Assert(GinPageIsLeaf(page));

	/*
	 * Iterate through all items on the page. As we progress, make note of
	 * the location of items at suitable intervals.
	 */
	ptr = beginptr = GinDataLeafPageGetPostingList(page);
	endptr = GinDataLeafPageGetPostingListEnd(page);
	MemSet(&iptr, 0, sizeof(ItemPointerData));

	nextptr = beginptr + (int) ((double) GinDataLeafMaxPostingListSize / (double) (GinDataLeafIndexCount + 1));
	j = 0;
	i = FirstOffsetNumber;
	readstate = 0;
	while (ptr < endptr && j < GinDataLeafIndexCount)
	{
		/* Place next page index entry if it's time */
		if (ptr >= nextptr)
		{
			indexes[j].iptr = iptr;
			indexes[j].offset = ptr - GinDataLeafPageGetPostingList(page);
			j++;
			nextptr = beginptr + (int) ((double) (j + 1) * (double) GinDataLeafMaxPostingListSize / (double) (GinDataLeafIndexCount + 1));
		}
		ptr = ginPostingListDecode(ptr, &iptr, &readstate);
		i++;
	}

	/* Clear the rest of the indexes to mark them unused. */
	memset(&indexes[j], 0, (GinDataLeafIndexCount - j) * sizeof(GinDataLeafItemIndex));
}

/*
 * Update page item indexes incrementally, after inserting 'len' bytes at
 * offset 'off'.
 *
 * Update indexes in the end of page. How do we update item indexes is
 * illustrated in following picture. Let item indexes 1 and 2 be item
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
void
ginUpdateItemIndexes(Page page, int off, int len)
{
	GinDataLeafItemIndex *indexes = GinPageGetIndexes(page);
	int			i;
	int			targetbin;

	Assert(GinPageIsData(page) && GinPageIsLeaf(page));

	/*
	 * First, adjust the offsets in any existing bins after the insert
	 * location.
	 */
	for (i = GinDataLeafIndexCount - 1; i >= 0; i--)
	{
		if (indexes[i].offset == 0)
			continue;

		if (indexes[i].offset < off)
			break;

		indexes[i].offset += len;
		Assert(indexes[i].offset < GinDataLeafPageGetPostingListSize(page));
	}
	targetbin = i + 1;

	/*
	 * The leaf item index is now consistent, and could be used for searching.
	 * However, the bin the new tuples fell into might've become
	 * disproportionally large. Check if it should be split.
	 */
	for (;;)
	{
		int			nextoff;
		int			prevoff;
		Pointer		beginptr;
		Pointer		ptr;
		ItemPointerData iptr;
		uint64		readstate;

		if (indexes[GinDataLeafIndexCount - 1].offset != 0)
			break; /* all slots are in use, can't insert a new one */

		if (indexes[targetbin].offset == 0)
			nextoff = GinDataLeafPageGetPostingListSize(page);
		else
			nextoff = indexes[targetbin].offset;
		if (targetbin == 0)
		{
			prevoff = 0;
			MemSet(&iptr, 0, sizeof(ItemPointerData));
		}
		else
		{
			prevoff = indexes[targetbin - 1].offset;
			iptr = indexes[targetbin - 1].iptr;
		}

		/*
		 * Is this bin large enough to split?
		 *
		 * The threshold is 2x the desired bin size, so that after the split
		 * both halves will have the right size.
		 */
		if (nextoff - prevoff < TARGET_BIN_SIZE * 2)
			break;

		/* Shift page item indexes to create a hole for a new one */
		memmove(&indexes[targetbin + 1], &indexes[targetbin],
				sizeof(GinDataLeafItemIndex) *
				(GinDataLeafIndexCount - targetbin - 1));

		/*
		 * Scan through the bin to find the split point. The bin is not
		 * split in the middle, but at the first TARGET_BIN_SIZE limit.
		 * That might leave the right bin still larger than the split
		 * threshold, in which case we will loop back to split it further.
		 */
		beginptr = GinDataLeafPageGetPostingList(page) + prevoff;
		ptr = beginptr;
		readstate = ginPostingListDecodeBegin(&iptr);
		while (ptr - beginptr < TARGET_BIN_SIZE)
			ptr = ginPostingListDecode(ptr, &iptr, &readstate);

		/* Add new bin */
		indexes[targetbin].iptr = iptr;
		indexes[targetbin].offset = ptr - GinDataLeafPageGetPostingList(page);
		Assert(indexes[targetbin].offset < GinDataLeafPageGetPostingListSize(page));

		/* Loop to possibly further split the next bin. */
		targetbin++;
	}
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
	int			i;
	CompressedPostingList ptr,
				cur;
	uint64		writestate;
	Size		freespace;
	int			nrootitems;

	/*
	 * Create the root page.
	 */
	buffer = GinNewBuffer(index);
	page = BufferGetPage(buffer);
	blkno = BufferGetBlockNumber(buffer);

	START_CRIT_SECTION();

	GinInitDataLeafPage(page);
	freespace = GinDataLeafPageGetFreeSpace(page);

	/*
	 * Write as many of the items to the root page as fit.
	 */
	ptr = GinDataLeafPageGetPostingList(page);
	cur = ptr;
	writestate = 0;
	for (i = 0; i < nitems && cur - ptr < freespace - MAX_COMPRESSED_ITEM_POINTER_SIZE; i++)
	{
		cur = ginPostingListEncode(cur, &items[i], &writestate);
	}
	nrootitems = i;
	GinDataLeafPageSetPostingListSize(page, cur - ptr);
	Assert(GinDataPageFreeSpacePre(page, cur) >= 0);
	ginRebuildItemIndexes(page);

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
	btree->splitPage = dataSplitPage;
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
