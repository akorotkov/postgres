/*-------------------------------------------------------------------------
 *
 * ginbtree.c
 *	  page utilities routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginbtree.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "miscadmin.h"
#include "utils/rel.h"

/*
 * Locks buffer by needed method for search.
 */
static int
ginTraverseLock(Buffer buffer, bool searchMode)
{
	Page		page;
	int			access = GIN_SHARE;

	LockBuffer(buffer, GIN_SHARE);
	page = BufferGetPage(buffer);
	if (GinPageIsLeaf(page))
	{
		if (searchMode == FALSE)
		{
			/* we should relock our page */
			LockBuffer(buffer, GIN_UNLOCK);
			LockBuffer(buffer, GIN_EXCLUSIVE);

			/* But root can become non-leaf during relock */
			if (!GinPageIsLeaf(page))
			{
				/* restore old lock type (very rare) */
				LockBuffer(buffer, GIN_UNLOCK);
				LockBuffer(buffer, GIN_SHARE);
			}
			else
				access = GIN_EXCLUSIVE;
		}
	}

	return access;
}

/*
 * Descends the tree to the leaf page that contains or would contain the
 * key we're searching for. The key should already be filled in 'btree',
 * in tree-type specific manner. If btree->fullScan is true, descends to the
 * leftmost leaf page.
 *
 * If 'searchmode' is false, on return stack->buffer is exclusively locked,
 * and the stack represents the full path to the root. Otherwise stack->buffer
 * is share-locked, and stack->parent is NULL.
 */
GinBtreeStack *
ginFindLeafPage(GinBtree btree, BlockNumber rootBlkno, bool searchMode)
{
	GinBtreeStack *stack;

	stack = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));
	stack->blkno = rootBlkno;
	stack->buffer = ReadBuffer(btree->index, rootBlkno);
	stack->parent = NULL;
	stack->predictNumber = 1;

	for (;;)
	{
		Page		page;
		BlockNumber child;
		int			access;

		stack->off = InvalidOffsetNumber;

		page = BufferGetPage(stack->buffer);

		access = ginTraverseLock(stack->buffer, searchMode);

		/*
		 * ok, page is correctly locked, we should check to move right ..,
		 * root never has a right link, so small optimization
		 */
		while (btree->fullScan == FALSE && stack->blkno != rootBlkno &&
			   btree->isMoveRight(btree, page))
		{
			BlockNumber rightlink = GinPageGetOpaque(page)->rightlink;

			if (rightlink == InvalidBlockNumber)
				/* rightmost page */
				break;

			stack->buffer = ginStepRight(stack->buffer, btree->index, access);
			stack->blkno = rightlink;
			page = BufferGetPage(stack->buffer);
		}

		if (GinPageIsLeaf(page))	/* we found, return locked page */
			return stack;

		/* now we have correct buffer, try to find child */
		child = btree->findChildPage(btree, stack);

		LockBuffer(stack->buffer, GIN_UNLOCK);
		Assert(child != InvalidBlockNumber);
		Assert(stack->blkno != child);

		if (searchMode)
		{
			/* in search mode we may forget path to leaf */
			stack->blkno = child;
			stack->buffer = ReleaseAndReadBuffer(stack->buffer, btree->index, stack->blkno);
		}
		else
		{
			GinBtreeStack *ptr = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));

			ptr->parent = stack;
			stack = ptr;
			stack->blkno = child;
			stack->buffer = ReadBuffer(btree->index, stack->blkno);
			stack->predictNumber = 1;
		}
	}
}

/*
 * Step right from current page.
 *
 * The next page is locked first, before releasing the current page. This is
 * crucial to protect from concurrent page deletion (see comment in
 * ginDeletePage).
 */
Buffer
ginStepRight(Buffer buffer, Relation index, int lockmode)
{
	Buffer		nextbuffer;
	Page		page = BufferGetPage(buffer);
	bool		isLeaf = GinPageIsLeaf(page);
	bool		isData = GinPageIsData(page);
	BlockNumber	blkno = GinPageGetOpaque(page)->rightlink;

	nextbuffer = ReadBuffer(index, blkno);
	LockBuffer(nextbuffer, lockmode);
	UnlockReleaseBuffer(buffer);

	/* Sanity check that the page we stepped to is of similar kind. */
	page = BufferGetPage(nextbuffer);
	if (isLeaf != GinPageIsLeaf(page) || isData != GinPageIsData(page))
		elog(ERROR, "right sibling of GIN page is of different type");

	/*
	 * Given the proper lock sequence above, we should never land on a
	 * deleted page.
	 */
	if  (GinPageIsDeleted(page))
		elog(ERROR, "right sibling of GIN page was deleted");

	return nextbuffer;
}

void
freeGinBtreeStack(GinBtreeStack *stack)
{
	while (stack)
	{
		GinBtreeStack *tmp = stack->parent;

		if (stack->buffer != InvalidBuffer)
			ReleaseBuffer(stack->buffer);

		pfree(stack);
		stack = tmp;
	}
}

/*
 * Try to find parent for current stack position, returns correct
 * parent and child's offset in  stack->parent.
 * Function should never release root page to prevent conflicts
 * with vacuum process
 */
void
ginFindParents(GinBtree btree, GinBtreeStack *stack,
			   BlockNumber rootBlkno)
{
	Page		page;
	Buffer		buffer;
	BlockNumber blkno,
				leftmostBlkno;
	OffsetNumber offset;
	GinBtreeStack *root = stack->parent;
	GinBtreeStack *ptr;

	if (!root)
	{
		/* XLog mode... */
		root = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));
		root->blkno = rootBlkno;
		root->buffer = ReadBuffer(btree->index, rootBlkno);
		LockBuffer(root->buffer, GIN_EXCLUSIVE);
		root->parent = NULL;
	}
	else
	{
		/*
		 * find root, we should not release root page until update is
		 * finished!!
		 */
		while (root->parent)
		{
			ReleaseBuffer(root->buffer);
			root = root->parent;
		}

		Assert(root->blkno == rootBlkno);
		Assert(BufferGetBlockNumber(root->buffer) == rootBlkno);
		LockBuffer(root->buffer, GIN_EXCLUSIVE);
	}
	root->off = InvalidOffsetNumber;

	page = BufferGetPage(root->buffer);
	Assert(!GinPageIsLeaf(page));

	/* check trivial case */
	if ((root->off = btree->findChildPtr(btree, page, stack->blkno, InvalidOffsetNumber)) != InvalidOffsetNumber)
	{
		stack->parent = root;
		return;
	}

	leftmostBlkno = blkno = btree->getLeftMostChild(btree, page);
	LockBuffer(root->buffer, GIN_UNLOCK);
	Assert(blkno != InvalidBlockNumber);

	for (;;)
	{
		buffer = ReadBuffer(btree->index, blkno);
		LockBuffer(buffer, GIN_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (GinPageIsLeaf(page))
			elog(ERROR, "Lost path");

		leftmostBlkno = btree->getLeftMostChild(btree, page);

		while ((offset = btree->findChildPtr(btree, page, stack->blkno, InvalidOffsetNumber)) == InvalidOffsetNumber)
		{
			blkno = GinPageGetOpaque(page)->rightlink;
			if (blkno == InvalidBlockNumber)
			{
				UnlockReleaseBuffer(buffer);
				break;
			}
			buffer = ginStepRight(buffer, btree->index, GIN_EXCLUSIVE);
			page = BufferGetPage(buffer);
		}

		if (blkno != InvalidBlockNumber)
		{
			ptr = (GinBtreeStack *) palloc(sizeof(GinBtreeStack));
			ptr->blkno = blkno;
			ptr->buffer = buffer;
			ptr->parent = root; /* it's may be wrong, but in next call we will
								 * correct */
			ptr->off = offset;
			stack->parent = ptr;
			return;
		}

		blkno = leftmostBlkno;
	}
}

/*
 * Returns true if the insertion is done, false if the page was split and
 * downlink insertion is pending.
 *
 * stack->buffer is locked on entry, and is kept locked.
 */
static bool
ginPlaceToPage(GinBtree btree, BlockNumber rootBlkno, GinBtreeStack *stack,
			   GinStatsData *buildStats)
{
	Page		page = BufferGetPage(stack->buffer);
	XLogRecData *rdata;
	bool		fit;

	START_CRIT_SECTION();
	fit = btree->placeToPage(btree, stack->buffer, stack->off, &rdata);
	if (fit)
	{
		MarkBufferDirty(stack->buffer);

		if (RelationNeedsWAL(btree->index))
		{
			XLogRecPtr	recptr;

			recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_INSERT, rdata);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		return true;
	}
	else
	{
		/* Didn't fit, have to split */
		Buffer		rbuffer;
		Page		newlpage;
		BlockNumber savedRightLink;
		GinBtreeStack *parent;
		Page		lpage,
					rpage;

		END_CRIT_SECTION();

		rbuffer = GinNewBuffer(btree->index);

		savedRightLink = GinPageGetOpaque(page)->rightlink;

		/*
		 * newlpage is a pointer to memory page, it is not associated with
		 * a buffer. stack->buffer is not touched yet.
		 */
		newlpage = btree->splitPage(btree, stack->buffer, rbuffer, stack->off, &rdata);

		((ginxlogSplit *) (rdata->data))->rootBlkno = rootBlkno;

		/* During index build, count the newly-split page */
		if (buildStats)
		{
			if (btree->isData)
				buildStats->nDataPages++;
			else
				buildStats->nEntryPages++;
		}

		parent = stack->parent;

		if (parent == NULL)
		{
			/*
			 * split root, so we need to allocate new left page and place
			 * pointer on root to left and right page
			 */
			Buffer		lbuffer = GinNewBuffer(btree->index);

			((ginxlogSplit *) (rdata->data))->isRootSplit = TRUE;
			((ginxlogSplit *) (rdata->data))->rrlink = InvalidBlockNumber;

			lpage = BufferGetPage(lbuffer);
			rpage = BufferGetPage(rbuffer);

			GinPageGetOpaque(rpage)->rightlink = InvalidBlockNumber;
			GinPageGetOpaque(newlpage)->rightlink = BufferGetBlockNumber(rbuffer);
			((ginxlogSplit *) (rdata->data))->lblkno = BufferGetBlockNumber(lbuffer);

			START_CRIT_SECTION();

			GinInitBuffer(stack->buffer, GinPageGetOpaque(newlpage)->flags & ~GIN_LEAF);
			PageRestoreTempPage(newlpage, lpage);
			btree->fillRoot(btree, stack->buffer, lbuffer, rbuffer);

			MarkBufferDirty(rbuffer);
			MarkBufferDirty(lbuffer);
			MarkBufferDirty(stack->buffer);

			if (RelationNeedsWAL(btree->index))
			{
				XLogRecPtr	recptr;

				recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_SPLIT, rdata);
				PageSetLSN(page, recptr);
				PageSetLSN(lpage, recptr);
				PageSetLSN(rpage, recptr);
			}

			UnlockReleaseBuffer(rbuffer);
			UnlockReleaseBuffer(lbuffer);
			END_CRIT_SECTION();

			/* During index build, count the newly-added root page */
			if (buildStats)
			{
				if (btree->isData)
					buildStats->nDataPages++;
				else
					buildStats->nEntryPages++;
			}

			return true;
		}
		else
		{
			/* split non-root page */
			((ginxlogSplit *) (rdata->data))->isRootSplit = FALSE;
			((ginxlogSplit *) (rdata->data))->rrlink = savedRightLink;

			lpage = BufferGetPage(stack->buffer);
			rpage = BufferGetPage(rbuffer);

			GinPageGetOpaque(rpage)->rightlink = savedRightLink;
			GinPageGetOpaque(newlpage)->rightlink = BufferGetBlockNumber(rbuffer);

			START_CRIT_SECTION();
			PageRestoreTempPage(newlpage, lpage);

			MarkBufferDirty(rbuffer);
			MarkBufferDirty(stack->buffer);

			if (RelationNeedsWAL(btree->index))
			{
				XLogRecPtr	recptr;

				recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_SPLIT, rdata);
				PageSetLSN(lpage, recptr);
				PageSetLSN(rpage, recptr);
			}
			UnlockReleaseBuffer(rbuffer);
			END_CRIT_SECTION();

			return false;
		}
	}
}

/*
 * Insert value (stored in GinBtree) to tree described by stack
 *
 * During an index build, buildStats is non-null and the counters
 * it contains are incremented as needed.
 *
 * NB: the passed-in stack is freed, as though by freeGinBtreeStack.
 */
void
ginInsertValue(GinBtree btree, GinBtreeStack *stack, GinStatsData *buildStats)
{
	GinBtreeStack *parent;
	BlockNumber rootBlkno;
	Page		page;

	/* extract root BlockNumber from stack */
	Assert(stack != NULL);
	parent = stack;
	while (parent->parent)
		parent = parent->parent;
	rootBlkno = parent->blkno;
	Assert(BlockNumberIsValid(rootBlkno));

	/* this loop crawls up the stack until the insertion is complete */
	for (;;)
	{
		bool done;

		done = ginPlaceToPage(btree, rootBlkno, stack, buildStats);

		/* just to be extra sure we don't delete anything by accident... */
		btree->isDelete = FALSE;

		if (done)
		{
			LockBuffer(stack->buffer, GIN_UNLOCK);
			freeGinBtreeStack(stack);
			break;
		}

		btree->prepareDownlink(btree, stack->buffer);

		/* search parent to lock */
		LockBuffer(parent->buffer, GIN_EXCLUSIVE);

		/* move right if it's needed */
		page = BufferGetPage(parent->buffer);
		while ((parent->off = btree->findChildPtr(btree, page, stack->blkno, parent->off)) == InvalidOffsetNumber)
		{
			BlockNumber rightlink = GinPageGetOpaque(page)->rightlink;

			if (rightlink == InvalidBlockNumber)
			{
				/*
				 * rightmost page, but we don't find parent, we should use
				 * plain search...
				 */
				LockBuffer(parent->buffer, GIN_UNLOCK);
				ginFindParents(btree, stack, rootBlkno);
				parent = stack->parent;
				Assert(parent != NULL);
				break;
			}

			parent->buffer = ginStepRight(parent->buffer, btree->index, GIN_EXCLUSIVE);
			parent->blkno = rightlink;
			page = BufferGetPage(parent->buffer);
		}

		UnlockReleaseBuffer(stack->buffer);
		pfree(stack);
		stack = parent;
	}
}
