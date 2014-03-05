/*-------------------------------------------------------------------------
 *
 * vodkaentrypage.c
 *	  routines for handling VODKA entry tree pages.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkaentrypage.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/vodka_private.h"
#include "miscadmin.h"
#include "utils/rel.h"

static void entrySplitPage(VodkaBtree btree, Buffer origbuf,
			   VodkaBtreeStack *stack,
			   void *insertPayload,
			   BlockNumber updateblkno, XLogRecData **prdata,
			   Page *newlpage, Page *newrpage);

/*
 * Form a tuple for entry tree.
 *
 * If the tuple would be too big to be stored, function throws a suitable
 * error if errorTooBig is TRUE, or returns NULL if errorTooBig is FALSE.
 *
 * See src/backend/access/vodka/README for a description of the index tuple
 * format that is being built here.  We build on the assumption that we
 * are making a leaf-level key entry containing a posting list of nipd items.
 * If the caller is actually trying to make a posting-tree entry, non-leaf
 * entry, or pending-list entry, it should pass dataSize = 0 and then overwrite
 * the t_tid fields as necessary.  In any case, 'data' can be NULL to skip
 * filling in the posting list; the caller is responsible for filling it
 * afterwards if data = NULL and nipd > 0.
 */
IndexTuple
VodkaFormTuple(VodkaState *vodkastate,
			 OffsetNumber attnum, Datum key, VodkaNullCategory category,
			 Pointer data, Size dataSize, int nipd,
			 bool errorTooBig)
{
	Datum		datums[2];
	bool		isnull[2];
	IndexTuple	itup;
	uint32		newsize;

	/* Build the basic tuple: optional column number, plus key datum */
	if (vodkastate->oneCol)
	{
		datums[0] = key;
		isnull[0] = (category != VODKA_CAT_NORM_KEY);
	}
	else
	{
		datums[0] = UInt16GetDatum(attnum);
		isnull[0] = false;
		datums[1] = key;
		isnull[1] = (category != VODKA_CAT_NORM_KEY);
	}

	itup = index_form_tuple(vodkastate->tupdesc[attnum - 1], datums, isnull);

	/*
	 * Determine and store offset to the posting list, making sure there is
	 * room for the category byte if needed.
	 *
	 * Note: because index_form_tuple MAXALIGNs the tuple size, there may well
	 * be some wasted pad space.  Is it worth recomputing the data length to
	 * prevent that?  That would also allow us to Assert that the real data
	 * doesn't overlap the VodkaNullCategory byte, which this code currently
	 * takes on faith.
	 */
	newsize = IndexTupleSize(itup);

	if (IndexTupleHasNulls(itup))
	{
		uint32		minsize;

		Assert(category != VODKA_CAT_NORM_KEY);
		minsize = VodkaCategoryOffset(itup, vodkastate) + sizeof(VodkaNullCategory);
		newsize = Max(newsize, minsize);
	}

	newsize = SHORTALIGN(newsize);

	VodkaSetPostingOffset(itup, newsize);
	VodkaSetNPosting(itup, nipd);

	/*
	 * Add space needed for posting list, if any.  Then check that the tuple
	 * won't be too big to store.
	 */
	newsize += dataSize;

	newsize = MAXALIGN(newsize);

	if (newsize > VodkaMaxItemSize)
	{
		if (errorTooBig)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
				   (Size) newsize, (Size) VodkaMaxItemSize,
				   RelationGetRelationName(vodkastate->index))));
		pfree(itup);
		return NULL;
	}

	/*
	 * Resize tuple if needed
	 */
	if (newsize != IndexTupleSize(itup))
	{
		itup = repalloc(itup, newsize);

		/*
		 * PostgreSQL 9.3 and earlier did not clear this new space, so we
		 * might find uninitialized padding when reading tuples from disk.
		 */
		memset((char *) itup + IndexTupleSize(itup),
			   0, newsize - IndexTupleSize(itup));
		/* set new size in tuple header */
		itup->t_info &= ~INDEX_SIZE_MASK;
		itup->t_info |= newsize;
	}

	/*
	 * Copy in the posting list, if provided
	 */
	if (data)
	{
		char *ptr = VodkaGetPosting(itup);
		memcpy(ptr, data, dataSize);
	}

	/*
	 * Insert category byte, if needed
	 */
	if (category != VODKA_CAT_NORM_KEY)
	{
		Assert(IndexTupleHasNulls(itup));
		VodkaSetNullCategory(itup, vodkastate, category);
	}
	return itup;
}

/*
 * Read item pointers from leaf entry tuple.
 *
 * Returns a palloc'd array of ItemPointers. The number of items is returned
 * in *nitems.
 */
ItemPointer
vodkaReadTuple(VodkaState *vodkastate, OffsetNumber attnum, IndexTuple itup,
			 int *nitems)
{
	Pointer		ptr = VodkaGetPosting(itup);
	int			nipd = VodkaGetNPosting(itup);
	ItemPointer	ipd;
	int			ndecoded;

	if (VodkaItupIsCompressed(itup))
	{
		if (nipd > 0)
		{
			ipd = vodkaPostingListDecode((VodkaPostingList *) ptr, &ndecoded);
			if (nipd != ndecoded)
				elog(ERROR, "number of items mismatch in VODKA entry tuple, %d in tuple header, %d decoded",
					 nipd, ndecoded);
		}
		else
		{
			ipd = palloc(0);
		}
	}
	else
	{
		ipd = (ItemPointer) palloc(sizeof(ItemPointerData) * nipd);
		memcpy(ipd, ptr, sizeof(ItemPointerData) * nipd);
	}
	*nitems = nipd;
	return ipd;
}

/*
 * Form a non-leaf entry tuple by copying the key data from the given tuple,
 * which can be either a leaf or non-leaf entry tuple.
 *
 * Any posting list in the source tuple is not copied.	The specified child
 * block number is inserted into t_tid.
 */
static IndexTuple
VodkaFormInteriorTuple(IndexTuple itup, Page page, BlockNumber childblk)
{
	IndexTuple	nitup;

	if (VodkaPageIsLeaf(page) && !VodkaIsPostingTree(itup))
	{
		/* Tuple contains a posting list, just copy stuff before that */
		uint32		origsize = VodkaGetPostingOffset(itup);

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
	VodkaSetDownlink(nitup, childblk);

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
entryIsMoveRight(VodkaBtree btree, Page page)
{
	IndexTuple	itup;
	OffsetNumber attnum;
	Datum		key;
	VodkaNullCategory category;

	if (VodkaPageRightMost(page))
		return FALSE;

	itup = getRightMostTuple(page);
	attnum = vodkatuple_get_attrnum(btree->vodkastate, itup);
	key = vodkatuple_get_key(btree->vodkastate, itup, &category);

	if (vodkaCompareAttEntries(btree->vodkastate,
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
entryLocateEntry(VodkaBtree btree, VodkaBtreeStack *stack)
{
	OffsetNumber low,
				high,
				maxoff;
	IndexTuple	itup = NULL;
	int			result;
	Page		page = BufferGetPage(stack->buffer);

	Assert(!VodkaPageIsLeaf(page));
	Assert(!VodkaPageIsData(page));

	if (btree->fullScan)
	{
		stack->off = FirstOffsetNumber;
		stack->predictNumber *= PageGetMaxOffsetNumber(page);
		return btree->getLeftMostChild(btree, page);
	}

	low = FirstOffsetNumber;
	maxoff = high = PageGetMaxOffsetNumber(page);
	Assert(high >= low);

	high++;

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		if (mid == maxoff && VodkaPageRightMost(page))
		{
			/* Right infinity */
			result = -1;
		}
		else
		{
			OffsetNumber attnum;
			Datum		key;
			VodkaNullCategory category;

			itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
			attnum = vodkatuple_get_attrnum(btree->vodkastate, itup);
			key = vodkatuple_get_key(btree->vodkastate, itup, &category);
			result = vodkaCompareAttEntries(btree->vodkastate,
										  btree->entryAttnum,
										  btree->entryKey,
										  btree->entryCategory,
										  attnum, key, category);
		}

		if (result == 0)
		{
			stack->off = mid;
			Assert(VodkaGetDownlink(itup) != VODKA_ROOT_BLKNO);
			return VodkaGetDownlink(itup);
		}
		else if (result > 0)
			low = mid + 1;
		else
			high = mid;
	}

	Assert(high >= FirstOffsetNumber && high <= maxoff);

	stack->off = high;
	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, high));
	Assert(VodkaGetDownlink(itup) != VODKA_ROOT_BLKNO);
	return VodkaGetDownlink(itup);
}

/*
 * Searches correct position for value on leaf page.
 * Page should be correctly chosen.
 * Returns true if value found on page.
 */
static bool
entryLocateLeafEntry(VodkaBtree btree, VodkaBtreeStack *stack)
{
	Page		page = BufferGetPage(stack->buffer);
	OffsetNumber low,
				high;

	Assert(VodkaPageIsLeaf(page));
	Assert(!VodkaPageIsData(page));

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
		VodkaNullCategory category;
		int			result;

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
		attnum = vodkatuple_get_attrnum(btree->vodkastate, itup);
		key = vodkatuple_get_key(btree->vodkastate, itup, &category);
		result = vodkaCompareAttEntries(btree->vodkastate,
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
entryFindChildPtr(VodkaBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
{
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(page);
	IndexTuple	itup;

	Assert(!VodkaPageIsLeaf(page));
	Assert(!VodkaPageIsData(page));

	/* if page isn't changed, we returns storedOff */
	if (storedOff >= FirstOffsetNumber && storedOff <= maxoff)
	{
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, storedOff));
		if (VodkaGetDownlink(itup) == blkno)
			return storedOff;

		/*
		 * we hope, that needed pointer goes to right. It's true if there
		 * wasn't a deletion
		 */
		for (i = storedOff + 1; i <= maxoff; i++)
		{
			itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
			if (VodkaGetDownlink(itup) == blkno)
				return i;
		}
		maxoff = storedOff - 1;
	}

	/* last chance */
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
		if (VodkaGetDownlink(itup) == blkno)
			return i;
	}

	return InvalidOffsetNumber;
}

static BlockNumber
entryGetLeftMostPage(VodkaBtree btree, Page page)
{
	IndexTuple	itup;

	Assert(!VodkaPageIsLeaf(page));
	Assert(!VodkaPageIsData(page));
	Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
	return VodkaGetDownlink(itup);
}

static bool
entryIsEnoughSpace(VodkaBtree btree, Buffer buf, OffsetNumber off,
				   VodkaBtreeEntryInsertData *insertData)
{
	Size		releasedsz = 0;
	Size		addedsz;
	Page		page = BufferGetPage(buf);

	Assert(insertData->entry);
	Assert(!VodkaPageIsData(page));

	if (insertData->isDelete)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

		releasedsz = MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
	}

	addedsz = MAXALIGN(IndexTupleSize(insertData->entry)) + sizeof(ItemIdData);

	if (PageGetFreeSpace(page) + releasedsz >= addedsz)
		return true;

	return false;
}

/*
 * Delete tuple on leaf page if tuples existed and we
 * should update it, update old child blkno to new right page
 * if child split occurred
 */
static void
entryPreparePage(VodkaBtree btree, Page page, OffsetNumber off,
				 VodkaBtreeEntryInsertData *insertData, BlockNumber updateblkno)
{
	Assert(insertData->entry);
	Assert(!VodkaPageIsData(page));

	if (insertData->isDelete)
	{
		Assert(VodkaPageIsLeaf(page));
		PageIndexTupleDelete(page, off);
	}

	if (!VodkaPageIsLeaf(page) && updateblkno != InvalidBlockNumber)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

		VodkaSetDownlink(itup, updateblkno);
	}
}

/*
 * Place tuple on page and fills WAL record
 *
 * If the tuple doesn't fit, returns false without modifying the page.
 *
 * On insertion to an internal node, in addition to inserting the given item,
 * the downlink of the existing item at 'off' is updated to point to
 * 'updateblkno'.
 */
static VodkaPlaceToPageRC
entryPlaceToPage(VodkaBtree btree, Buffer buf, VodkaBtreeStack *stack,
				 void *insertPayload, BlockNumber updateblkno,
				 XLogRecData **prdata, Page *newlpage, Page *newrpage)
{
	VodkaBtreeEntryInsertData *insertData = insertPayload;
	Page		page = BufferGetPage(buf);
	OffsetNumber off = stack->off;
	OffsetNumber placed;
	int			cnt = 0;

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[3];
	static vodkaxlogInsertEntry data;

	/* quick exit if it doesn't fit */
	if (!entryIsEnoughSpace(btree, buf, off, insertData))
	{
		entrySplitPage(btree, buf, stack, insertPayload, updateblkno,
					   prdata, newlpage, newrpage);
		return SPLIT;
	}

	START_CRIT_SECTION();

	*prdata = rdata;
	entryPreparePage(btree, page, off, insertData, updateblkno);

	placed = PageAddItem(page,
						 (Item) insertData->entry,
						 IndexTupleSize(insertData->entry),
						 off, false, false);
	if (placed != off)
		elog(ERROR, "failed to add item to index page in \"%s\"",
			 RelationGetRelationName(btree->index));

	data.isDelete = insertData->isDelete;
	data.offset = off;

	rdata[cnt].buffer = buf;
	rdata[cnt].buffer_std = false;
	rdata[cnt].data = (char *) &data;
	rdata[cnt].len = offsetof(vodkaxlogInsertEntry, tuple);
	rdata[cnt].next = &rdata[cnt + 1];
	cnt++;

	rdata[cnt].buffer = buf;
	rdata[cnt].buffer_std = false;
	rdata[cnt].data = (char *) insertData->entry;
	rdata[cnt].len = IndexTupleSize(insertData->entry);
	rdata[cnt].next = NULL;

	return INSERTED;
}

/*
 * Place tuple and split page, orivodkaal buffer(lbuf) leaves untouched,
 * returns shadow pages filled with new data.
 * Tuples are distributed between pages by equal size on its, not
 * an equal number!
 */
static void
entrySplitPage(VodkaBtree btree, Buffer origbuf,
			   VodkaBtreeStack *stack,
			   void *insertPayload,
			   BlockNumber updateblkno, XLogRecData **prdata,
			   Page *newlpage, Page *newrpage)
{
	VodkaBtreeEntryInsertData *insertData = insertPayload;
	OffsetNumber off = stack->off;
	OffsetNumber i,
				maxoff,
				separator = InvalidOffsetNumber;
	Size		totalsize = 0;
	Size		tupstoresize;
	Size		lsize = 0,
				size;
	char	   *ptr;
	IndexTuple	itup;
	Page		page;
	Page		lpage = PageGetTempPageCopy(BufferGetPage(origbuf));
	Page		rpage = PageGetTempPageCopy(BufferGetPage(origbuf));
	Size		pageSize = PageGetPageSize(lpage);

	/* these must be static so they can be returned to caller */
	static XLogRecData rdata[2];
	static vodkaxlogSplitEntry data;
	static char tupstore[2 * BLCKSZ];

	*prdata = rdata;
	entryPreparePage(btree, lpage, off, insertData, updateblkno);

	/*
	 * First, append all the existing tuples and the new tuple we're inserting
	 * one after another in a temporary workspace.
	 */
	maxoff = PageGetMaxOffsetNumber(lpage);
	ptr = tupstore;
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		if (i == off)
		{
			size = MAXALIGN(IndexTupleSize(insertData->entry));
			memcpy(ptr, insertData->entry, size);
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
		size = MAXALIGN(IndexTupleSize(insertData->entry));
		memcpy(ptr, insertData->entry, size);
		ptr += size;
		totalsize += size + sizeof(ItemIdData);
	}
	tupstoresize = ptr - tupstore;

	/*
	 * Initialize the left and right pages, and copy all the tuples back to
	 * them.
	 */
	VodkaInitPage(rpage, VodkaPageGetOpaque(lpage)->flags, pageSize);
	VodkaInitPage(lpage, VodkaPageGetOpaque(rpage)->flags, pageSize);

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
			lsize += MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
		}

		if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to index page in \"%s\"",
				 RelationGetRelationName(btree->index));
		ptr += MAXALIGN(IndexTupleSize(itup));
	}

	data.separator = separator;
	data.nitem = maxoff;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(vodkaxlogSplitEntry);
	rdata[0].next = &rdata[1];

	rdata[1].buffer = InvalidBuffer;
	rdata[1].data = tupstore;
	rdata[1].len = tupstoresize;
	rdata[1].next = NULL;

	*newlpage = lpage;
	*newrpage = rpage;
}

/*
 * Construct insertion payload for inserting the downlink for given buffer.
 */
static void *
entryPrepareDownlink(VodkaBtree btree, Buffer lbuf)
{
	VodkaBtreeEntryInsertData *insertData;
	Page		lpage = BufferGetPage(lbuf);
	BlockNumber lblkno = BufferGetBlockNumber(lbuf);
	IndexTuple	itup;

	itup = getRightMostTuple(lpage);

	insertData = palloc(sizeof(VodkaBtreeEntryInsertData));
	insertData->entry = VodkaFormInteriorTuple(itup, lpage, lblkno);
	insertData->isDelete = false;

	return insertData;
}

/*
 * Fills new root by rightest values from child.
 * Also called from vodkaxlog, should not use btree
 */
void
vodkaEntryFillRoot(VodkaBtree btree, Page root,
				 BlockNumber lblkno, Page lpage,
				 BlockNumber rblkno, Page rpage)
{
	IndexTuple	itup;

	itup = VodkaFormInteriorTuple(getRightMostTuple(lpage), lpage, lblkno);
	if (PageAddItem(root, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to index root page");
	pfree(itup);

	itup = VodkaFormInteriorTuple(getRightMostTuple(rpage), rpage, rblkno);
	if (PageAddItem(root, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to index root page");
	pfree(itup);
}

/*
 * Set up VodkaBtree for entry page access
 *
 * Note: during WAL recovery, there may be no valid data in vodkastate
 * other than a faked-up Relation pointer; the key datum is bogus too.
 */
void
vodkaPrepareEntryScan(VodkaBtree btree, OffsetNumber attnum,
					Datum key, VodkaNullCategory category,
					VodkaState *vodkastate)
{
	memset(btree, 0, sizeof(VodkaBtreeData));

	btree->index = vodkastate->index;
	btree->rootBlkno = VODKA_ROOT_BLKNO;
	btree->vodkastate = vodkastate;

	btree->findChildPage = entryLocateEntry;
	btree->getLeftMostChild = entryGetLeftMostPage;
	btree->isMoveRight = entryIsMoveRight;
	btree->findItem = entryLocateLeafEntry;
	btree->findChildPtr = entryFindChildPtr;
	btree->placeToPage = entryPlaceToPage;
	btree->fillRoot = vodkaEntryFillRoot;
	btree->prepareDownlink = entryPrepareDownlink;

	btree->isData = FALSE;
	btree->fullScan = FALSE;
	btree->isBuild = FALSE;

	btree->entryAttnum = attnum;
	btree->entryKey = key;
	btree->entryCategory = category;
}
