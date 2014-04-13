/*-------------------------------------------------------------------------
 *
 * vodkaxlog.c
 *	  WAL replay logic for inverted index.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/backend/access/vodka/vodkaxlog.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/vodka_private.h"
#include "access/xlogutils.h"
#include "utils/memutils.h"

static MemoryContext opCtx;		/* working memory for operations */

static void
vodkaRedoClearIncompleteSplit(XLogRecPtr lsn, RelFileNode node, BlockNumber blkno)
{
	Buffer		buffer;
	Page		page;

	buffer = XLogReadBuffer(node, blkno, false);
	if (!BufferIsValid(buffer))
		return;					/* page was deleted, nothing to do */
	page = (Page) BufferGetPage(buffer);

	if (lsn > PageGetLSN(page))
	{
		VodkaPageGetOpaque(page)->flags &= ~VODKA_INCOMPLETE_SPLIT;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	UnlockReleaseBuffer(buffer);
}

static void
vodkaRedoCreateIndex(XLogRecPtr lsn, XLogRecord *record)
{
	RelFileNode *node = (RelFileNode *) XLogRecGetData(record);
	Buffer		RootBuffer,
				MetaBuffer;
	Page		page;

	/* Backup blocks are not used in create_index records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	MetaBuffer = XLogReadBuffer(*node, VODKA_METAPAGE_BLKNO, true);
	Assert(BufferIsValid(MetaBuffer));
	page = (Page) BufferGetPage(MetaBuffer);

	VodkaInitMetabuffer(NULL, MetaBuffer, InvalidOid);

	PageSetLSN(page, lsn);
	MarkBufferDirty(MetaBuffer);

	RootBuffer = XLogReadBuffer(*node, VODKA_ROOT_BLKNO, true);
	Assert(BufferIsValid(RootBuffer));
	page = (Page) BufferGetPage(RootBuffer);

	VodkaInitBuffer(RootBuffer, VODKA_LEAF);

	PageSetLSN(page, lsn);
	MarkBufferDirty(RootBuffer);

	UnlockReleaseBuffer(RootBuffer);
	UnlockReleaseBuffer(MetaBuffer);
}

static void
vodkaRedoCreatePTree(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogCreatePostingTree *data = (vodkaxlogCreatePostingTree *) XLogRecGetData(record);
	char	   *ptr;
	Buffer		buffer;
	Page		page;

	/* Backup blocks are not used in create_ptree records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	buffer = XLogReadBuffer(data->node, data->blkno, true);
	Assert(BufferIsValid(buffer));
	page = (Page) BufferGetPage(buffer);

	VodkaInitBuffer(buffer, VODKA_DATA | VODKA_LEAF | VODKA_COMPRESSED);

	ptr = XLogRecGetData(record) + sizeof(vodkaxlogCreatePostingTree);

	/* Place page data */
	memcpy(VodkaDataLeafPageGetPostingList(page), ptr, data->size);

	VodkaDataLeafPageSetPostingListSize(page, data->size);

	PageSetLSN(page, lsn);

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
vodkaRedoInsertEntry(Buffer buffer, bool isLeaf, BlockNumber rightblkno, void *rdata)
{
	Page		page = BufferGetPage(buffer);
	vodkaxlogInsertEntry *data = (vodkaxlogInsertEntry *) rdata;
	OffsetNumber offset = data->offset;
	IndexTuple	itup;

	if (rightblkno != InvalidBlockNumber)
	{
		/* update link to right page after split */
		Assert(!VodkaPageIsLeaf(page));
		Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offset));
		VodkaSetDownlink(itup, rightblkno);
	}

	if (data->isDelete)
	{
		Assert(VodkaPageIsLeaf(page));
		Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
		PageIndexTupleDelete(page, offset);
	}

	itup = &data->tuple;

	if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), offset, false, false) == InvalidOffsetNumber)
	{
		RelFileNode node;
		ForkNumber forknum;
		BlockNumber blknum;

		BufferGetTag(buffer, &node, &forknum, &blknum);
		elog(ERROR, "failed to add item to index page in %u/%u/%u",
			 node.spcNode, node.dbNode, node.relNode);
	}
}

static void
vodkaRedoRecompress(Page page, vodkaxlogRecompressDataLeaf *data)
{
	int			actionno;
	int			segno;
	VodkaPostingList *oldseg;
	Pointer		segmentend;
	char	   *walbuf;
	int			totalsize;

	/*
	 * If the page is in pre-9.4 format, convert to new format first.
	 */
	if (!VodkaPageIsCompressed(page))
	{
		ItemPointer uncompressed = (ItemPointer) VodkaDataPageGetData(page);
		int			nuncompressed = VodkaPageGetOpaque(page)->maxoff;
		int			npacked;
		VodkaPostingList *plist;

		plist = vodkaCompressPostingList(uncompressed, nuncompressed,
									   BLCKSZ, &npacked);
		Assert(npacked == nuncompressed);

		totalsize = SizeOfVodkaPostingList(plist);

		memcpy(VodkaDataLeafPageGetPostingList(page), plist, totalsize);
		VodkaDataLeafPageSetPostingListSize(page, totalsize);
		VodkaPageSetCompressed(page);
		VodkaPageGetOpaque(page)->maxoff = InvalidOffsetNumber;
	}

	oldseg = VodkaDataLeafPageGetPostingList(page);
	segmentend = (Pointer) oldseg + VodkaDataLeafPageGetPostingListSize(page);
	segno = 0;

	walbuf = ((char *) data) + sizeof(vodkaxlogRecompressDataLeaf);
	for (actionno = 0; actionno < data->nactions; actionno++)
	{
		uint8		a_segno = *((uint8 *) (walbuf++));
		uint8		a_action = *((uint8 *) (walbuf++));
		VodkaPostingList *newseg = NULL;
		int			newsegsize = 0;
		ItemPointerData *items = NULL;
		uint16		nitems = 0;
		ItemPointerData *olditems;
		int			nolditems;
		ItemPointerData *newitems;
		int			nnewitems;
		int			segsize;
		Pointer		segptr;
		int			szleft;

		/* Extract all the information we need from the WAL record */
		if (a_action == VODKA_SEGMENT_INSERT ||
			a_action == VODKA_SEGMENT_REPLACE)
		{
			newseg = (VodkaPostingList *) walbuf;
			newsegsize = SizeOfVodkaPostingList(newseg);
			walbuf += SHORTALIGN(newsegsize);
		}

		if (a_action == VODKA_SEGMENT_ADDITEMS)
		{
			memcpy(&nitems, walbuf, sizeof(uint16));
			walbuf += sizeof(uint16);
			items = (ItemPointerData *) walbuf;
			walbuf += nitems * sizeof(ItemPointerData);
		}

		/* Skip to the segment that this action concerns */
		Assert(segno <= a_segno);
		while (segno < a_segno)
		{
			oldseg = VodkaNextPostingListSegment(oldseg);
			segno++;
		}

		/*
		 * ADDITEMS action is handled like REPLACE, but the new segment to
		 * replace the old one is reconstructed using the old segment from
		 * disk and the new items from the WAL record.
		 */
		if (a_action == VODKA_SEGMENT_ADDITEMS)
		{
			int			npacked;

			olditems = vodkaPostingListDecode(oldseg, &nolditems);

			newitems = vodkaMergeItemPointers(items, nitems,
											olditems, nolditems,
											&nnewitems);
			Assert(nnewitems == nolditems + nitems);

			newseg = vodkaCompressPostingList(newitems, nnewitems,
											BLCKSZ, &npacked);
			Assert(npacked == nnewitems);

			newsegsize = SizeOfVodkaPostingList(newseg);
			a_action = VODKA_SEGMENT_REPLACE;
		}

		segptr = (Pointer) oldseg;
		if (segptr != segmentend)
			segsize = SizeOfVodkaPostingList(oldseg);
		else
		{
			/*
			 * Positioned after the last existing segment. Only INSERTs
			 * expected here.
			 */
			Assert(a_action == VODKA_SEGMENT_INSERT);
			segsize = 0;
		}
		szleft = segmentend - segptr;

		switch (a_action)
		{
			case VODKA_SEGMENT_DELETE:
				memmove(segptr, segptr + segsize, szleft - segsize);
				segmentend -= segsize;

				segno++;
				break;

			case VODKA_SEGMENT_INSERT:
				/* make room for the new segment */
				memmove(segptr + newsegsize, segptr, szleft);
				/* copy the new segment in place */
				memcpy(segptr, newseg, newsegsize);
				segmentend += newsegsize;
				segptr += newsegsize;
				break;

			case VODKA_SEGMENT_REPLACE:
				/* shift the segments that follow */
				memmove(segptr + newsegsize,
						segptr + segsize,
						szleft - segsize);
				/* copy the replacement segment in place */
				memcpy(segptr, newseg, newsegsize);
				segmentend -= segsize;
				segmentend += newsegsize;
				segptr += newsegsize;
				segno++;
				break;

			default:
				elog(ERROR, "unexpected VODKA leaf action: %u", a_action);
		}
		oldseg = (VodkaPostingList *) segptr;
	}

	totalsize = segmentend - (Pointer) VodkaDataLeafPageGetPostingList(page);
	VodkaDataLeafPageSetPostingListSize(page, totalsize);
}

static void
vodkaRedoInsertData(Buffer buffer, bool isLeaf, BlockNumber rightblkno, void *rdata)
{
	Page		page = BufferGetPage(buffer);

	if (isLeaf)
	{
		vodkaxlogRecompressDataLeaf *data = (vodkaxlogRecompressDataLeaf *) rdata;

		Assert(VodkaPageIsLeaf(page));

		vodkaRedoRecompress(page, data);
	}
	else
	{
		vodkaxlogInsertDataInternal *data = (vodkaxlogInsertDataInternal *) rdata;
		PostingItem *oldpitem;

		Assert(!VodkaPageIsLeaf(page));

		/* update link to right page after split */
		oldpitem = VodkaDataPageGetPostingItem(page, data->offset);
		PostingItemSetBlockNumber(oldpitem, rightblkno);

		VodkaDataPageAddPostingItem(page, &data->newitem, data->offset);
	}
}

static void
vodkaRedoInsert(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogInsert *data = (vodkaxlogInsert *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	char	   *payload;
	BlockNumber leftChildBlkno = InvalidBlockNumber;
	BlockNumber rightChildBlkno = InvalidBlockNumber;
	bool		isLeaf = (data->flags & VODKA_INSERT_ISLEAF) != 0;

	payload = XLogRecGetData(record) + sizeof(vodkaxlogInsert);

	/*
	 * First clear incomplete-split flag on child page if this finishes
	 * a split.
	 */
	if (!isLeaf)
	{
		leftChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
		payload += sizeof(BlockIdData);
		rightChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
		payload += sizeof(BlockIdData);

		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
			vodkaRedoClearIncompleteSplit(lsn, data->node, leftChildBlkno);
	}

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(isLeaf ? 0 : 1))
	{
		(void) RestoreBackupBlock(lsn, record, isLeaf ? 0 : 1, false, false);
		return;
	}

	buffer = XLogReadBuffer(data->node, data->blkno, false);
	if (!BufferIsValid(buffer))
		return;					/* page was deleted, nothing to do */
	page = (Page) BufferGetPage(buffer);

	if (lsn > PageGetLSN(page))
	{
		/* How to insert the payload is tree-type specific */
		if (data->flags & VODKA_INSERT_ISDATA)
		{
			Assert(VodkaPageIsData(page));
			vodkaRedoInsertData(buffer, isLeaf, rightChildBlkno, payload);
		}
		else
		{
			Assert(!VodkaPageIsData(page));
			vodkaRedoInsertEntry(buffer, isLeaf, rightChildBlkno, payload);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	UnlockReleaseBuffer(buffer);
}

static void
vodkaRedoSplitEntry(Page lpage, Page rpage, void *rdata)
{
	vodkaxlogSplitEntry *data = (vodkaxlogSplitEntry *) rdata;
	IndexTuple	itup = (IndexTuple) ((char *) rdata + sizeof(vodkaxlogSplitEntry));
	OffsetNumber i;

	for (i = 0; i < data->separator; i++)
	{
		if (PageAddItem(lpage, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to vodka index page");
		itup = (IndexTuple) (((char *) itup) + MAXALIGN(IndexTupleSize(itup)));
	}

	for (i = data->separator; i < data->nitem; i++)
	{
		if (PageAddItem(rpage, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to vodka index page");
		itup = (IndexTuple) (((char *) itup) + MAXALIGN(IndexTupleSize(itup)));
	}
}

static void
vodkaRedoSplitData(Page lpage, Page rpage, void *rdata)
{
	bool		isleaf = VodkaPageIsLeaf(lpage);

	if (isleaf)
	{
		vodkaxlogSplitDataLeaf *data = (vodkaxlogSplitDataLeaf *) rdata;
		Pointer		lptr = (Pointer) rdata + sizeof(vodkaxlogSplitDataLeaf);
		Pointer		rptr = lptr + data->lsize;

		Assert(data->lsize > 0 && data->lsize <= VodkaDataLeafMaxContentSize);
		Assert(data->rsize > 0 && data->rsize <= VodkaDataLeafMaxContentSize);

		memcpy(VodkaDataLeafPageGetPostingList(lpage), lptr, data->lsize);
		memcpy(VodkaDataLeafPageGetPostingList(rpage), rptr, data->rsize);

		VodkaDataLeafPageSetPostingListSize(lpage, data->lsize);
		VodkaDataLeafPageSetPostingListSize(rpage, data->rsize);
		*VodkaDataPageGetRightBound(lpage) = data->lrightbound;
		*VodkaDataPageGetRightBound(rpage) = data->rrightbound;
	}
	else
	{
		vodkaxlogSplitDataInternal *data = (vodkaxlogSplitDataInternal *) rdata;
		PostingItem *items = (PostingItem *) ((char *) rdata + sizeof(vodkaxlogSplitDataInternal));
		OffsetNumber i;
		OffsetNumber maxoff;

		for (i = 0; i < data->separator; i++)
			VodkaDataPageAddPostingItem(lpage, &items[i], InvalidOffsetNumber);
		for (i = data->separator; i < data->nitem; i++)
			VodkaDataPageAddPostingItem(rpage, &items[i], InvalidOffsetNumber);

		/* set up right key */
		maxoff = VodkaPageGetOpaque(lpage)->maxoff;
		*VodkaDataPageGetRightBound(lpage) = VodkaDataPageGetPostingItem(lpage, maxoff)->key;
		*VodkaDataPageGetRightBound(rpage) = data->rightbound;
	}
}

static void
vodkaRedoSplit(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogSplit *data = (vodkaxlogSplit *) XLogRecGetData(record);
	Buffer		lbuffer,
				rbuffer;
	Page		lpage,
				rpage;
	uint32		flags;
	uint32		lflags,
				rflags;
	char	   *payload;
	bool		isLeaf = (data->flags & VODKA_INSERT_ISLEAF) != 0;
	bool		isData = (data->flags & VODKA_INSERT_ISDATA) != 0;
	bool		isRoot = (data->flags & VODKA_SPLIT_ROOT) != 0;

	payload = XLogRecGetData(record) + sizeof(vodkaxlogSplit);

	/*
	 * First clear incomplete-split flag on child page if this finishes
	 * a split
	 */
	if (!isLeaf)
	{
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
			vodkaRedoClearIncompleteSplit(lsn, data->node, data->leftChildBlkno);
	}

	flags = 0;
	if (isLeaf)
		flags |= VODKA_LEAF;
	if (isData)
		flags |= VODKA_DATA;
	if (isLeaf && isData)
		flags |= VODKA_COMPRESSED;

	lflags = rflags = flags;
	if (!isRoot)
		lflags |= VODKA_INCOMPLETE_SPLIT;

	lbuffer = XLogReadBuffer(data->node, data->lblkno, true);
	Assert(BufferIsValid(lbuffer));
	lpage = (Page) BufferGetPage(lbuffer);
	VodkaInitBuffer(lbuffer, lflags);

	rbuffer = XLogReadBuffer(data->node, data->rblkno, true);
	Assert(BufferIsValid(rbuffer));
	rpage = (Page) BufferGetPage(rbuffer);
	VodkaInitBuffer(rbuffer, rflags);

	VodkaPageGetOpaque(lpage)->rightlink = BufferGetBlockNumber(rbuffer);
	VodkaPageGetOpaque(rpage)->rightlink = isRoot ? InvalidBlockNumber : data->rrlink;

	/* Do the tree-type specific portion to restore the page contents */
	if (isData)
		vodkaRedoSplitData(lpage, rpage, payload);
	else
		vodkaRedoSplitEntry(lpage, rpage, payload);

	PageSetLSN(rpage, lsn);
	MarkBufferDirty(rbuffer);

	PageSetLSN(lpage, lsn);
	MarkBufferDirty(lbuffer);

	if (isRoot)
	{
		BlockNumber	rootBlkno = data->rrlink;
		Buffer		rootBuf = XLogReadBuffer(data->node, rootBlkno, true);
		Page		rootPage = BufferGetPage(rootBuf);

		VodkaInitBuffer(rootBuf, flags & ~VODKA_LEAF & ~VODKA_COMPRESSED);

		if (isData)
		{
			Assert(rootBlkno != VODKA_ROOT_BLKNO);
			vodkaDataFillRoot(NULL, BufferGetPage(rootBuf),
							BufferGetBlockNumber(lbuffer),
							BufferGetPage(lbuffer),
							BufferGetBlockNumber(rbuffer),
							BufferGetPage(rbuffer));
		}
		else
		{
			Assert(rootBlkno == VODKA_ROOT_BLKNO);
			vodkaEntryFillRoot(NULL, BufferGetPage(rootBuf),
							 BufferGetBlockNumber(lbuffer),
							 BufferGetPage(lbuffer),
							 BufferGetBlockNumber(rbuffer),
							 BufferGetPage(rbuffer));
		}

		PageSetLSN(rootPage, lsn);

		MarkBufferDirty(rootBuf);
		UnlockReleaseBuffer(rootBuf);
	}

	UnlockReleaseBuffer(rbuffer);
	UnlockReleaseBuffer(lbuffer);
}

/*
 * This is functionally the same as heap_xlog_newpage.
 */
static void
vodkaRedoVacuumPage(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogVacuumPage *xlrec = (vodkaxlogVacuumPage *) XLogRecGetData(record);
	char	   *blk = ((char *) xlrec) + sizeof(vodkaxlogVacuumPage);
	Buffer		buffer;
	Page		page;

	Assert(xlrec->hole_offset < BLCKSZ);
	Assert(xlrec->hole_length < BLCKSZ);

	/* Backup blocks are not used, we'll re-initialize the page always. */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	buffer = XLogReadBuffer(xlrec->node, xlrec->blkno, true);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	if (xlrec->hole_length == 0)
	{
		memcpy((char *) page, blk, BLCKSZ);
	}
	else
	{
		memcpy((char *) page, blk, xlrec->hole_offset);
		/* must zero-fill the hole */
		MemSet((char *) page + xlrec->hole_offset, 0, xlrec->hole_length);
		memcpy((char *) page + (xlrec->hole_offset + xlrec->hole_length),
			   blk + xlrec->hole_offset,
			   BLCKSZ - (xlrec->hole_offset + xlrec->hole_length));
	}

	PageSetLSN(page, lsn);

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
vodkaRedoVacuumDataLeafPage(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogVacuumDataLeafPage *xlrec = (vodkaxlogVacuumDataLeafPage *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(xlrec->node, xlrec->blkno, false);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	Assert(VodkaPageIsLeaf(page));
	Assert(VodkaPageIsData(page));

	if (lsn > PageGetLSN(page))
	{
		vodkaRedoRecompress(page, &xlrec->data);
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	UnlockReleaseBuffer(buffer);
}

static void
vodkaRedoDeletePage(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogDeletePage *data = (vodkaxlogDeletePage *) XLogRecGetData(record);
	Buffer		dbuffer;
	Buffer		pbuffer;
	Buffer		lbuffer;
	Page		page;

	if (record->xl_info & XLR_BKP_BLOCK(0))
		dbuffer = RestoreBackupBlock(lsn, record, 0, false, true);
	else
	{
		dbuffer = XLogReadBuffer(data->node, data->blkno, false);
		if (BufferIsValid(dbuffer))
		{
			page = BufferGetPage(dbuffer);
			if (lsn > PageGetLSN(page))
			{
				Assert(VodkaPageIsData(page));
				VodkaPageGetOpaque(page)->flags = VODKA_DELETED;
				PageSetLSN(page, lsn);
				MarkBufferDirty(dbuffer);
			}
		}
	}

	if (record->xl_info & XLR_BKP_BLOCK(1))
		pbuffer = RestoreBackupBlock(lsn, record, 1, false, true);
	else
	{
		pbuffer = XLogReadBuffer(data->node, data->parentBlkno, false);
		if (BufferIsValid(pbuffer))
		{
			page = BufferGetPage(pbuffer);
			if (lsn > PageGetLSN(page))
			{
				Assert(VodkaPageIsData(page));
				Assert(!VodkaPageIsLeaf(page));
				VodkaPageDeletePostingItem(page, data->parentOffset);
				PageSetLSN(page, lsn);
				MarkBufferDirty(pbuffer);
			}
		}
	}

	if (record->xl_info & XLR_BKP_BLOCK(2))
		(void) RestoreBackupBlock(lsn, record, 2, false, false);
	else if (data->leftBlkno != InvalidBlockNumber)
	{
		lbuffer = XLogReadBuffer(data->node, data->leftBlkno, false);
		if (BufferIsValid(lbuffer))
		{
			page = BufferGetPage(lbuffer);
			if (lsn > PageGetLSN(page))
			{
				Assert(VodkaPageIsData(page));
				VodkaPageGetOpaque(page)->rightlink = data->rightLink;
				PageSetLSN(page, lsn);
				MarkBufferDirty(lbuffer);
			}
			UnlockReleaseBuffer(lbuffer);
		}
	}

	if (BufferIsValid(pbuffer))
		UnlockReleaseBuffer(pbuffer);
	if (BufferIsValid(dbuffer))
		UnlockReleaseBuffer(dbuffer);
}

static void
vodkaRedoUpdateMetapage(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogUpdateMeta *data = (vodkaxlogUpdateMeta *) XLogRecGetData(record);
	Buffer		metabuffer;
	Page		metapage;
	Buffer		buffer;

	/*
	 * Restore the metapage. This is essentially the same as a full-page image,
	 * so restore the metapage unconditionally without looking at the LSN, to
	 * avoid torn page hazards.
	 */
	metabuffer = XLogReadBuffer(data->node, VODKA_METAPAGE_BLKNO, false);
	if (!BufferIsValid(metabuffer))
		return;					/* assume index was deleted, nothing to do */
	metapage = BufferGetPage(metabuffer);

	memcpy(VodkaPageGetMeta(metapage), &data->metadata, sizeof(VodkaMetaPageData));
	PageSetLSN(metapage, lsn);
	MarkBufferDirty(metabuffer);

	if (data->ntuples > 0)
	{
		/*
		 * insert into tail page
		 */
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
		{
			buffer = XLogReadBuffer(data->node, data->metadata.tail, false);
			if (BufferIsValid(buffer))
			{
				Page		page = BufferGetPage(buffer);

				if (lsn > PageGetLSN(page))
				{
					OffsetNumber l,
								off = (PageIsEmpty(page)) ? FirstOffsetNumber :
					OffsetNumberNext(PageGetMaxOffsetNumber(page));
					int			i,
								tupsize;
					IndexTuple	tuples = (IndexTuple) (XLogRecGetData(record) + sizeof(vodkaxlogUpdateMeta));

					for (i = 0; i < data->ntuples; i++)
					{
						tupsize = IndexTupleSize(tuples);

						l = PageAddItem(page, (Item) tuples, tupsize, off, false, false);

						if (l == InvalidOffsetNumber)
							elog(ERROR, "failed to add item to index page");

						tuples = (IndexTuple) (((char *) tuples) + tupsize);

						off++;
					}

					/*
					 * Increase counter of heap tuples
					 */
					VodkaPageGetOpaque(page)->maxoff++;

					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
			}
		}
	}
	else if (data->prevTail != InvalidBlockNumber)
	{
		/*
		 * New tail
		 */
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
		{
			buffer = XLogReadBuffer(data->node, data->prevTail, false);
			if (BufferIsValid(buffer))
			{
				Page		page = BufferGetPage(buffer);

				if (lsn > PageGetLSN(page))
				{
					VodkaPageGetOpaque(page)->rightlink = data->newRightlink;

					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
			}
		}
	}

	UnlockReleaseBuffer(metabuffer);
}

static void
vodkaRedoInsertListPage(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogInsertListPage *data = (vodkaxlogInsertListPage *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber l,
				off = FirstOffsetNumber;
	int			i,
				tupsize;
	IndexTuple	tuples = (IndexTuple) (XLogRecGetData(record) + sizeof(vodkaxlogInsertListPage));

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(data->node, data->blkno, true);
	Assert(BufferIsValid(buffer));
	page = BufferGetPage(buffer);

	VodkaInitBuffer(buffer, VODKA_LIST);
	VodkaPageGetOpaque(page)->rightlink = data->rightlink;
	if (data->rightlink == InvalidBlockNumber)
	{
		/* tail of sublist */
		VodkaPageSetFullRow(page);
		VodkaPageGetOpaque(page)->maxoff = 1;
	}
	else
	{
		VodkaPageGetOpaque(page)->maxoff = 0;
	}

	for (i = 0; i < data->ntuples; i++)
	{
		tupsize = IndexTupleSize(tuples);

		l = PageAddItem(page, (Item) tuples, tupsize, off, false, false);

		if (l == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to index page");

		tuples = (IndexTuple) (((char *) tuples) + tupsize);
	}

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);

	UnlockReleaseBuffer(buffer);
}

static void
vodkaRedoDeleteListPages(XLogRecPtr lsn, XLogRecord *record)
{
	vodkaxlogDeleteListPages *data = (vodkaxlogDeleteListPages *) XLogRecGetData(record);
	Buffer		metabuffer;
	Page		metapage;
	int			i;

	/* Backup blocks are not used in delete_listpage records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	metabuffer = XLogReadBuffer(data->node, VODKA_METAPAGE_BLKNO, false);
	if (!BufferIsValid(metabuffer))
		return;					/* assume index was deleted, nothing to do */
	metapage = BufferGetPage(metabuffer);

	memcpy(VodkaPageGetMeta(metapage), &data->metadata, sizeof(VodkaMetaPageData));
	PageSetLSN(metapage, lsn);
	MarkBufferDirty(metabuffer);

	/*
	 * In normal operation, shiftList() takes exclusive lock on all the
	 * pages-to-be-deleted simultaneously.	During replay, however, it should
	 * be all right to lock them one at a time.  This is dependent on the fact
	 * that we are deleting pages from the head of the list, and that readers
	 * share-lock the next page before releasing the one they are on. So we
	 * cannot get past a reader that is on, or due to visit, any page we are
	 * going to delete.  New incoming readers will block behind our metapage
	 * lock and then see a fully updated page list.
	 */
	for (i = 0; i < data->ndeleted; i++)
	{
		Buffer		buffer = XLogReadBuffer(data->node, data->toDelete[i], false);

		if (BufferIsValid(buffer))
		{
			Page		page = BufferGetPage(buffer);

			if (lsn > PageGetLSN(page))
			{
				VodkaPageGetOpaque(page)->flags = VODKA_DELETED;

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}

			UnlockReleaseBuffer(buffer);
		}
	}
	UnlockReleaseBuffer(metabuffer);
}

void
vodka_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	MemoryContext oldCtx;

	/*
	 * VODKA indexes do not require any conflict processing. NB: If we ever
	 * implement a similar optimization as we have in b-tree, and remove
	 * killed tuples outside VACUUM, we'll need to handle that here.
	 */

	oldCtx = MemoryContextSwitchTo(opCtx);
	switch (info)
	{
		case XLOG_VODKA_CREATE_INDEX:
			vodkaRedoCreateIndex(lsn, record);
			break;
		case XLOG_VODKA_CREATE_PTREE:
			vodkaRedoCreatePTree(lsn, record);
			break;
		case XLOG_VODKA_INSERT:
			vodkaRedoInsert(lsn, record);
			break;
		case XLOG_VODKA_SPLIT:
			vodkaRedoSplit(lsn, record);
			break;
		case XLOG_VODKA_VACUUM_PAGE:
			vodkaRedoVacuumPage(lsn, record);
			break;
		case XLOG_VODKA_VACUUM_DATA_LEAF_PAGE:
			vodkaRedoVacuumDataLeafPage(lsn, record);
			break;
		case XLOG_VODKA_DELETE_PAGE:
			vodkaRedoDeletePage(lsn, record);
			break;
		case XLOG_VODKA_UPDATE_META_PAGE:
			vodkaRedoUpdateMetapage(lsn, record);
			break;
		case XLOG_VODKA_INSERT_LISTPAGE:
			vodkaRedoInsertListPage(lsn, record);
			break;
		case XLOG_VODKA_DELETE_LISTPAGE:
			vodkaRedoDeleteListPages(lsn, record);
			break;
		default:
			elog(PANIC, "vodka_redo: unknown op code %u", info);
	}
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(opCtx);
}

void
vodka_xlog_startup(void)
{
	opCtx = AllocSetContextCreate(CurrentMemoryContext,
								  "VODKA recovery temporary context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
}

void
vodka_xlog_cleanup(void)
{
	MemoryContextDelete(opCtx);
}
