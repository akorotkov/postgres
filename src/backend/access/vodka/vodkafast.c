/*-------------------------------------------------------------------------
 *
 * vodkafast.c
 *	  Fast insert routines for the Postgres inverted index access method.
 *	  Pending entries are stored in linear list of pages.  Later on
 *	  (typically during VACUUM), vodkaInsertCleanup() will be invoked to
 *	  transfer pending entries into the regular index structure.  This
 *	  wins because bulk insertion is much more efficient than retail.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkafast.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/vodka_private.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"


#define VODKA_PAGE_FREESIZE \
	( BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(VodkaPageOpaqueData)) )

typedef struct KeyArray
{
	Datum	   *keys;			/* expansible array */
	VodkaNullCategory *categories;	/* another expansible array */
	int32		nvalues;		/* current number of valid entries */
	int32		maxvalues;		/* allocated size of arrays */
} KeyArray;


/*
 * Build a pending-list page from the given array of tuples, and write it out.
 *
 * Returns amount of free space left on the page.
 */
static int32
writeListPage(Relation index, Buffer buffer,
			  IndexTuple *tuples, int32 ntuples, BlockNumber rightlink)
{
	Page		page = BufferGetPage(buffer);
	int32		i,
				freesize,
				size = 0;
	OffsetNumber l,
				off;
	char	   *workspace;
	char	   *ptr;

	/* workspace could be a local array; we use palloc for alignment */
	workspace = palloc(BLCKSZ);

	START_CRIT_SECTION();

	VodkaInitBuffer(buffer, VODKA_LIST);

	off = FirstOffsetNumber;
	ptr = workspace;

	for (i = 0; i < ntuples; i++)
	{
		int			this_size = IndexTupleSize(tuples[i]);

		memcpy(ptr, tuples[i], this_size);
		ptr += this_size;
		size += this_size;

		l = PageAddItem(page, (Item) tuples[i], this_size, off, false, false);

		if (l == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to index page in \"%s\"",
				 RelationGetRelationName(index));

		off++;
	}

	Assert(size <= BLCKSZ);		/* else we overran workspace */

	VodkaPageGetOpaque(page)->rightlink = rightlink;

	/*
	 * tail page may contain only whole row(s) or final part of row placed on
	 * previous pages (a "row" here meaning all the index tuples generated for
	 * one heap tuple)
	 */
	if (rightlink == InvalidBlockNumber)
	{
		VodkaPageSetFullRow(page);
		VodkaPageGetOpaque(page)->maxoff = 1;
	}
	else
	{
		VodkaPageGetOpaque(page)->maxoff = 0;
	}

	MarkBufferDirty(buffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecData rdata[2];
		vodkaxlogInsertListPage data;
		XLogRecPtr	recptr;

		data.node = index->rd_node;
		data.blkno = BufferGetBlockNumber(buffer);
		data.rightlink = rightlink;
		data.ntuples = ntuples;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &data;
		rdata[0].len = sizeof(vodkaxlogInsertListPage);
		rdata[0].next = rdata + 1;

		rdata[1].buffer = buffer;
		rdata[1].buffer_std = true;
		rdata[1].data = workspace;
		rdata[1].len = size;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_INSERT_LISTPAGE, rdata);
		PageSetLSN(page, recptr);
	}

	/* get free space before releasing buffer */
	freesize = PageGetExactFreeSpace(page);

	UnlockReleaseBuffer(buffer);

	END_CRIT_SECTION();

	pfree(workspace);

	return freesize;
}

static void
makeSublist(Relation index, IndexTuple *tuples, int32 ntuples,
			VodkaMetaPageData *res)
{
	Buffer		curBuffer = InvalidBuffer;
	Buffer		prevBuffer = InvalidBuffer;
	int			i,
				size = 0,
				tupsize;
	int			startTuple = 0;

	Assert(ntuples > 0);

	/*
	 * Split tuples into pages
	 */
	for (i = 0; i < ntuples; i++)
	{
		if (curBuffer == InvalidBuffer)
		{
			curBuffer = VodkaNewBuffer(index);

			if (prevBuffer != InvalidBuffer)
			{
				res->nPendingPages++;
				writeListPage(index, prevBuffer,
							  tuples + startTuple,
							  i - startTuple,
							  BufferGetBlockNumber(curBuffer));
			}
			else
			{
				res->head = BufferGetBlockNumber(curBuffer);
			}

			prevBuffer = curBuffer;
			startTuple = i;
			size = 0;
		}

		tupsize = MAXALIGN(IndexTupleSize(tuples[i])) + sizeof(ItemIdData);

		if (size + tupsize > VodkaListPageSize)
		{
			/* won't fit, force a new page and reprocess */
			i--;
			curBuffer = InvalidBuffer;
		}
		else
		{
			size += tupsize;
		}
	}

	/*
	 * Write last page
	 */
	res->tail = BufferGetBlockNumber(curBuffer);
	res->tailFreeSize = writeListPage(index, curBuffer,
									  tuples + startTuple,
									  ntuples - startTuple,
									  InvalidBlockNumber);
	res->nPendingPages++;
	/* that was only one heap tuple */
	res->nPendingHeapTuples = 1;
}

/*
 * Write the index tuples contained in *collector into the index's
 * pending list.
 *
 * Function guarantees that all these tuples will be inserted consecutively,
 * preserving order
 */
void
vodkaHeapTupleFastInsert(VodkaState *vodkastate, VodkaTupleCollector *collector)
{
	Relation	index = vodkastate->index;
	Buffer		metabuffer;
	Page		metapage;
	VodkaMetaPageData *metadata = NULL;
	XLogRecData rdata[2];
	Buffer		buffer = InvalidBuffer;
	Page		page = NULL;
	vodkaxlogUpdateMeta data;
	bool		separateList = false;
	bool		needCleanup = false;

	if (collector->ntuples == 0)
		return;

	data.node = index->rd_node;
	data.ntuples = 0;
	data.newRightlink = data.prevTail = InvalidBlockNumber;

	rdata[0].buffer = InvalidBuffer;
	rdata[0].data = (char *) &data;
	rdata[0].len = sizeof(vodkaxlogUpdateMeta);
	rdata[0].next = NULL;

	metabuffer = ReadBuffer(index, VODKA_METAPAGE_BLKNO);
	metapage = BufferGetPage(metabuffer);

	if (collector->sumsize + collector->ntuples * sizeof(ItemIdData) > VodkaListPageSize)
	{
		/*
		 * Total size is greater than one page => make sublist
		 */
		separateList = true;
	}
	else
	{
		LockBuffer(metabuffer, VODKA_EXCLUSIVE);
		metadata = VodkaPageGetMeta(metapage);

		if (metadata->head == InvalidBlockNumber ||
			collector->sumsize + collector->ntuples * sizeof(ItemIdData) > metadata->tailFreeSize)
		{
			/*
			 * Pending list is empty or total size is greater than freespace
			 * on tail page => make sublist
			 *
			 * We unlock metabuffer to keep high concurrency
			 */
			separateList = true;
			LockBuffer(metabuffer, VODKA_UNLOCK);
		}
	}

	if (separateList)
	{
		/*
		 * We should make sublist separately and append it to the tail
		 */
		VodkaMetaPageData sublist;

		memset(&sublist, 0, sizeof(VodkaMetaPageData));
		makeSublist(index, collector->tuples, collector->ntuples, &sublist);

		/*
		 * metapage was unlocked, see above
		 */
		LockBuffer(metabuffer, VODKA_EXCLUSIVE);
		metadata = VodkaPageGetMeta(metapage);

		if (metadata->head == InvalidBlockNumber)
		{
			/*
			 * Main list is empty, so just insert sublist as main list
			 */
			START_CRIT_SECTION();

			metadata->head = sublist.head;
			metadata->tail = sublist.tail;
			metadata->tailFreeSize = sublist.tailFreeSize;

			metadata->nPendingPages = sublist.nPendingPages;
			metadata->nPendingHeapTuples = sublist.nPendingHeapTuples;
		}
		else
		{
			/*
			 * Merge lists
			 */
			data.prevTail = metadata->tail;
			data.newRightlink = sublist.head;

			buffer = ReadBuffer(index, metadata->tail);
			LockBuffer(buffer, VODKA_EXCLUSIVE);
			page = BufferGetPage(buffer);

			rdata[0].next = rdata + 1;

			rdata[1].buffer = buffer;
			rdata[1].buffer_std = true;
			rdata[1].data = NULL;
			rdata[1].len = 0;
			rdata[1].next = NULL;

			Assert(VodkaPageGetOpaque(page)->rightlink == InvalidBlockNumber);

			START_CRIT_SECTION();

			VodkaPageGetOpaque(page)->rightlink = sublist.head;

			MarkBufferDirty(buffer);

			metadata->tail = sublist.tail;
			metadata->tailFreeSize = sublist.tailFreeSize;

			metadata->nPendingPages += sublist.nPendingPages;
			metadata->nPendingHeapTuples += sublist.nPendingHeapTuples;
		}
	}
	else
	{
		/*
		 * Insert into tail page.  Metapage is already locked
		 */
		OffsetNumber l,
					off;
		int			i,
					tupsize;
		char	   *ptr;

		buffer = ReadBuffer(index, metadata->tail);
		LockBuffer(buffer, VODKA_EXCLUSIVE);
		page = BufferGetPage(buffer);

		off = (PageIsEmpty(page)) ? FirstOffsetNumber :
			OffsetNumberNext(PageGetMaxOffsetNumber(page));

		rdata[0].next = rdata + 1;

		rdata[1].buffer = buffer;
		rdata[1].buffer_std = true;
		ptr = rdata[1].data = (char *) palloc(collector->sumsize);
		rdata[1].len = collector->sumsize;
		rdata[1].next = NULL;

		data.ntuples = collector->ntuples;

		START_CRIT_SECTION();

		/*
		 * Increase counter of heap tuples
		 */
		Assert(VodkaPageGetOpaque(page)->maxoff <= metadata->nPendingHeapTuples);
		VodkaPageGetOpaque(page)->maxoff++;
		metadata->nPendingHeapTuples++;

		for (i = 0; i < collector->ntuples; i++)
		{
			tupsize = IndexTupleSize(collector->tuples[i]);
			l = PageAddItem(page, (Item) collector->tuples[i], tupsize, off, false, false);

			if (l == InvalidOffsetNumber)
				elog(ERROR, "failed to add item to index page in \"%s\"",
					 RelationGetRelationName(index));

			memcpy(ptr, collector->tuples[i], tupsize);
			ptr += tupsize;

			off++;
		}

		Assert((ptr - rdata[1].data) <= collector->sumsize);

		metadata->tailFreeSize = PageGetExactFreeSpace(page);

		MarkBufferDirty(buffer);
	}

	/*
	 * Write metabuffer, make xlog entry
	 */
	MarkBufferDirty(metabuffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;

		memcpy(&data.metadata, metadata, sizeof(VodkaMetaPageData));

		recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_UPDATE_META_PAGE, rdata);
		PageSetLSN(metapage, recptr);

		if (buffer != InvalidBuffer)
		{
			PageSetLSN(page, recptr);
		}
	}

	if (buffer != InvalidBuffer)
		UnlockReleaseBuffer(buffer);

	/*
	 * Force pending list cleanup when it becomes too long. And,
	 * vodkaInsertCleanup could take significant amount of time, so we prefer to
	 * call it when it can do all the work in a single collection cycle. In
	 * non-vacuum mode, it shouldn't require maintenance_work_mem, so fire it
	 * while pending list is still small enough to fit into work_mem.
	 *
	 * vodkaInsertCleanup() should not be called inside our CRIT_SECTION.
	 */
	if (metadata->nPendingPages * VODKA_PAGE_FREESIZE > work_mem * 1024L)
		needCleanup = true;

	UnlockReleaseBuffer(metabuffer);

	END_CRIT_SECTION();

	if (needCleanup)
		vodkaInsertCleanup(vodkastate, false, NULL);
}

/*
 * Create temporary index tuples for a single indexable item (one index column
 * for the heap tuple specified by ht_ctid), and append them to the array
 * in *collector.  They will subsequently be written out using
 * vodkaHeapTupleFastInsert.	Note that to guarantee consistent state, all
 * temp tuples for a given heap tuple must be written in one call to
 * vodkaHeapTupleFastInsert.
 */
void
vodkaHeapTupleFastCollect(VodkaState *vodkastate,
						VodkaTupleCollector *collector,
						OffsetNumber attnum, Datum value, bool isNull,
						ItemPointer ht_ctid)
{
	Datum	   *entries;
	VodkaNullCategory *categories;
	int32		i,
				nentries;

	/*
	 * Extract the key values that need to be inserted in the index
	 */
	entries = vodkaExtractEntries(vodkastate, attnum, value, isNull,
								&nentries, &categories);

	/*
	 * Allocate/reallocate memory for storing collected tuples
	 */
	if (collector->tuples == NULL)
	{
		collector->lentuples = nentries * vodkastate->origTupdesc->natts;
		collector->tuples = (IndexTuple *) palloc(sizeof(IndexTuple) * collector->lentuples);
	}

	while (collector->ntuples + nentries > collector->lentuples)
	{
		collector->lentuples *= 2;
		collector->tuples = (IndexTuple *) repalloc(collector->tuples,
								  sizeof(IndexTuple) * collector->lentuples);
	}

	/*
	 * Build an index tuple for each key value, and add to array.  In pending
	 * tuples we just stick the heap TID into t_tid.
	 */
	for (i = 0; i < nentries; i++)
	{
		IndexTuple	itup;

		itup = VodkaFormTuple(vodkastate, attnum, entries[i], categories[i],
							NULL, 0, 0, true);
		itup->t_tid = *ht_ctid;
		collector->tuples[collector->ntuples++] = itup;
		collector->sumsize += IndexTupleSize(itup);
	}
}

/*
 * Deletes pending list pages up to (not including) newHead page.
 * If newHead == InvalidBlockNumber then function drops the whole list.
 *
 * metapage is pinned and exclusive-locked throughout this function.
 *
 * Returns true if another cleanup process is running concurrently
 * (if so, we can just abandon our own efforts)
 */
static bool
shiftList(Relation index, Buffer metabuffer, BlockNumber newHead,
		  IndexBulkDeleteResult *stats)
{
	Page		metapage;
	VodkaMetaPageData *metadata;
	BlockNumber blknoToDelete;

	metapage = BufferGetPage(metabuffer);
	metadata = VodkaPageGetMeta(metapage);
	blknoToDelete = metadata->head;

	do
	{
		Page		page;
		int			i;
		int64		nDeletedHeapTuples = 0;
		vodkaxlogDeleteListPages data;
		XLogRecData rdata[1];
		Buffer		buffers[VODKA_NDELETE_AT_ONCE];

		data.node = index->rd_node;

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &data;
		rdata[0].len = sizeof(vodkaxlogDeleteListPages);
		rdata[0].next = NULL;

		data.ndeleted = 0;
		while (data.ndeleted < VODKA_NDELETE_AT_ONCE && blknoToDelete != newHead)
		{
			data.toDelete[data.ndeleted] = blknoToDelete;
			buffers[data.ndeleted] = ReadBuffer(index, blknoToDelete);
			LockBuffer(buffers[data.ndeleted], VODKA_EXCLUSIVE);
			page = BufferGetPage(buffers[data.ndeleted]);

			data.ndeleted++;

			if (VodkaPageIsDeleted(page))
			{
				/* concurrent cleanup process is detected */
				for (i = 0; i < data.ndeleted; i++)
					UnlockReleaseBuffer(buffers[i]);

				return true;
			}

			nDeletedHeapTuples += VodkaPageGetOpaque(page)->maxoff;
			blknoToDelete = VodkaPageGetOpaque(page)->rightlink;
		}

		if (stats)
			stats->pages_deleted += data.ndeleted;

		START_CRIT_SECTION();

		metadata->head = blknoToDelete;

		Assert(metadata->nPendingPages >= data.ndeleted);
		metadata->nPendingPages -= data.ndeleted;
		Assert(metadata->nPendingHeapTuples >= nDeletedHeapTuples);
		metadata->nPendingHeapTuples -= nDeletedHeapTuples;

		if (blknoToDelete == InvalidBlockNumber)
		{
			metadata->tail = InvalidBlockNumber;
			metadata->tailFreeSize = 0;
			metadata->nPendingPages = 0;
			metadata->nPendingHeapTuples = 0;
		}

		MarkBufferDirty(metabuffer);

		for (i = 0; i < data.ndeleted; i++)
		{
			page = BufferGetPage(buffers[i]);
			VodkaPageGetOpaque(page)->flags = VODKA_DELETED;
			MarkBufferDirty(buffers[i]);
		}

		if (RelationNeedsWAL(index))
		{
			XLogRecPtr	recptr;

			memcpy(&data.metadata, metadata, sizeof(VodkaMetaPageData));

			recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_DELETE_LISTPAGE, rdata);
			PageSetLSN(metapage, recptr);

			for (i = 0; i < data.ndeleted; i++)
			{
				page = BufferGetPage(buffers[i]);
				PageSetLSN(page, recptr);
			}
		}

		for (i = 0; i < data.ndeleted; i++)
			UnlockReleaseBuffer(buffers[i]);

		END_CRIT_SECTION();
	} while (blknoToDelete != newHead);

	return false;
}

/* Initialize empty KeyArray */
static void
initKeyArray(KeyArray *keys, int32 maxvalues)
{
	keys->keys = (Datum *) palloc(sizeof(Datum) * maxvalues);
	keys->categories = (VodkaNullCategory *)
		palloc(sizeof(VodkaNullCategory) * maxvalues);
	keys->nvalues = 0;
	keys->maxvalues = maxvalues;
}

/* Add datum to KeyArray, resizing if needed */
static void
addDatum(KeyArray *keys, Datum datum, VodkaNullCategory category)
{
	if (keys->nvalues >= keys->maxvalues)
	{
		keys->maxvalues *= 2;
		keys->keys = (Datum *)
			repalloc(keys->keys, sizeof(Datum) * keys->maxvalues);
		keys->categories = (VodkaNullCategory *)
			repalloc(keys->categories, sizeof(VodkaNullCategory) * keys->maxvalues);
	}

	keys->keys[keys->nvalues] = datum;
	keys->categories[keys->nvalues] = category;
	keys->nvalues++;
}

/*
 * Collect data from a pending-list page in preparation for insertion into
 * the main index.
 *
 * Go through all tuples >= startoff on page and collect values in accum
 *
 * Note that ka is just workspace --- it does not carry any state across
 * calls.
 */
static void
processPendingPage(BuildAccumulator *accum, KeyArray *ka,
				   Page page, OffsetNumber startoff)
{
	ItemPointerData heapptr;
	OffsetNumber i,
				maxoff;
	OffsetNumber attrnum;

	/* reset *ka to empty */
	ka->nvalues = 0;

	maxoff = PageGetMaxOffsetNumber(page);
	Assert(maxoff >= FirstOffsetNumber);
	ItemPointerSetInvalid(&heapptr);
	attrnum = 0;

	for (i = startoff; i <= maxoff; i = OffsetNumberNext(i))
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
		OffsetNumber curattnum;
		Datum		curkey;
		VodkaNullCategory curcategory;

		/* Check for change of heap TID or attnum */
		curattnum = vodkatuple_get_attrnum(accum->vodkastate, itup);

		if (!ItemPointerIsValid(&heapptr))
		{
			heapptr = itup->t_tid;
			attrnum = curattnum;
		}
		else if (!(ItemPointerEquals(&heapptr, &itup->t_tid) &&
				   curattnum == attrnum))
		{
			/*
			 * vodkaInsertBAEntries can insert several datums per call, but only
			 * for one heap tuple and one column.  So call it at a boundary,
			 * and reset ka.
			 */
			vodkaInsertBAEntries(accum, &heapptr, attrnum,
							   ka->keys, ka->categories, ka->nvalues);
			ka->nvalues = 0;
			heapptr = itup->t_tid;
			attrnum = curattnum;
		}

		/* Add key to KeyArray */
		curkey = vodkatuple_get_key(accum->vodkastate, itup, &curcategory);
		addDatum(ka, curkey, curcategory);
	}

	/* Dump out all remaining keys */
	vodkaInsertBAEntries(accum, &heapptr, attrnum,
					   ka->keys, ka->categories, ka->nvalues);
}

/*
 * Move tuples from pending pages into regular VODKA structure.
 *
 * This can be called concurrently by multiple backends, so it must cope.
 * On first glance it looks completely not concurrent-safe and not crash-safe
 * either.	The reason it's okay is that multiple insertion of the same entry
 * is detected and treated as a no-op by vodkainsert.c.  If we crash after
 * posting entries to the main index and before removing them from the
 * pending list, it's okay because when we redo the posting later on, nothing
 * bad will happen.  Likewise, if two backends simultaneously try to post
 * a pending entry into the main index, one will succeed and one will do
 * nothing.  We try to notice when someone else is a little bit ahead of
 * us in the process, but that's just to avoid wasting cycles.  Only the
 * action of removing a page from the pending list really needs exclusive
 * lock.
 *
 * vac_delay indicates that vodkaInsertCleanup is called from vacuum process,
 * so call vacuum_delay_point() periodically.
 * If stats isn't null, we count deleted pending pages into the counts.
 */
void
vodkaInsertCleanup(VodkaState *vodkastate,
				 bool vac_delay, IndexBulkDeleteResult *stats)
{
	Relation	index = vodkastate->index;
	Buffer		metabuffer,
				buffer;
	Page		metapage,
				page;
	VodkaMetaPageData *metadata;
	MemoryContext opCtx,
				oldCtx;
	BuildAccumulator accum;
	KeyArray	datums;
	BlockNumber blkno;

	metabuffer = ReadBuffer(index, VODKA_METAPAGE_BLKNO);
	LockBuffer(metabuffer, VODKA_SHARE);
	metapage = BufferGetPage(metabuffer);
	metadata = VodkaPageGetMeta(metapage);

	if (metadata->head == InvalidBlockNumber)
	{
		/* Nothing to do */
		UnlockReleaseBuffer(metabuffer);
		return;
	}

	/*
	 * Read and lock head of pending list
	 */
	blkno = metadata->head;
	buffer = ReadBuffer(index, blkno);
	LockBuffer(buffer, VODKA_SHARE);
	page = BufferGetPage(buffer);

	LockBuffer(metabuffer, VODKA_UNLOCK);

	/*
	 * Initialize.	All temporary space will be in opCtx
	 */
	opCtx = AllocSetContextCreate(CurrentMemoryContext,
								  "VODKA insert cleanup temporary context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(opCtx);

	initKeyArray(&datums, 128);
	vodkaInitBA(&accum);
	accum.vodkastate = vodkastate;

	/*
	 * At the top of this loop, we have pin and lock on the current page of
	 * the pending list.  However, we'll release that before exiting the loop.
	 * Note we also have pin but not lock on the metapage.
	 */
	for (;;)
	{
		if (VodkaPageIsDeleted(page))
		{
			/* another cleanup process is running concurrently */
			UnlockReleaseBuffer(buffer);
			break;
		}

		/*
		 * read page's datums into accum
		 */
		processPendingPage(&accum, &datums, page, FirstOffsetNumber);

		if (vac_delay)
			vacuum_delay_point();

		/*
		 * Is it time to flush memory to disk?	Flush if we are at the end of
		 * the pending list, or if we have a full row and memory is getting
		 * full.
		 *
		 * XXX using up maintenance_work_mem here is probably unreasonably
		 * much, since vacuum might already be using that much.
		 */
		if (VodkaPageGetOpaque(page)->rightlink == InvalidBlockNumber ||
			(VodkaPageHasFullRow(page) &&
			 (accum.allocatedMemory >= maintenance_work_mem * 1024L)))
		{
			ItemPointerData *list;
			uint32		nlist;
			Datum		key;
			VodkaNullCategory category;
			OffsetNumber maxoff,
						attnum;

			/*
			 * Unlock current page to increase performance. Changes of page
			 * will be checked later by comparing maxoff after completion of
			 * memory flush.
			 */
			maxoff = PageGetMaxOffsetNumber(page);
			LockBuffer(buffer, VODKA_UNLOCK);

			/*
			 * Moving collected data into regular structure can take
			 * significant amount of time - so, run it without locking pending
			 * list.
			 */
			vodkaBeginBAScan(&accum);
			while ((list = vodkaGetBAEntry(&accum,
								  &attnum, &key, &category, &nlist)) != NULL)
			{
				vodkaEntryInsert(vodkastate, attnum, key, category,
							   list, nlist, NULL);
				if (vac_delay)
					vacuum_delay_point();
			}

			/*
			 * Lock the whole list to remove pages
			 */
			LockBuffer(metabuffer, VODKA_EXCLUSIVE);
			LockBuffer(buffer, VODKA_SHARE);

			if (VodkaPageIsDeleted(page))
			{
				/* another cleanup process is running concurrently */
				UnlockReleaseBuffer(buffer);
				LockBuffer(metabuffer, VODKA_UNLOCK);
				break;
			}

			/*
			 * While we left the page unlocked, more stuff might have gotten
			 * added to it.  If so, process those entries immediately.	There
			 * shouldn't be very many, so we don't worry about the fact that
			 * we're doing this with exclusive lock. Insertion algorithm
			 * guarantees that inserted row(s) will not continue on next page.
			 * NOTE: intentionally no vacuum_delay_point in this loop.
			 */
			if (PageGetMaxOffsetNumber(page) != maxoff)
			{
				vodkaInitBA(&accum);
				processPendingPage(&accum, &datums, page, maxoff + 1);

				vodkaBeginBAScan(&accum);
				while ((list = vodkaGetBAEntry(&accum,
								  &attnum, &key, &category, &nlist)) != NULL)
					vodkaEntryInsert(vodkastate, attnum, key, category,
								   list, nlist, NULL);
			}

			/*
			 * Remember next page - it will become the new list head
			 */
			blkno = VodkaPageGetOpaque(page)->rightlink;
			UnlockReleaseBuffer(buffer);		/* shiftList will do exclusive
												 * locking */

			/*
			 * remove readed pages from pending list, at this point all
			 * content of readed pages is in regular structure
			 */
			if (shiftList(index, metabuffer, blkno, stats))
			{
				/* another cleanup process is running concurrently */
				LockBuffer(metabuffer, VODKA_UNLOCK);
				break;
			}

			Assert(blkno == metadata->head);
			LockBuffer(metabuffer, VODKA_UNLOCK);

			/*
			 * if we removed the whole pending list just exit
			 */
			if (blkno == InvalidBlockNumber)
				break;

			/*
			 * release memory used so far and reinit state
			 */
			MemoryContextReset(opCtx);
			initKeyArray(&datums, datums.maxvalues);
			vodkaInitBA(&accum);
		}
		else
		{
			blkno = VodkaPageGetOpaque(page)->rightlink;
			UnlockReleaseBuffer(buffer);
		}

		/*
		 * Read next page in pending list
		 */
		CHECK_FOR_INTERRUPTS();
		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, VODKA_SHARE);
		page = BufferGetPage(buffer);
	}

	ReleaseBuffer(metabuffer);

	/* Clean up temporary space */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(opCtx);
}
