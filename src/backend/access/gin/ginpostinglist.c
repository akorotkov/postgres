/*-------------------------------------------------------------------------
 *
 * ginpostinglist.c
 *	  routines for dealing with posting lists.
 *
 *
 * XXX: Explain the varbyte encoding here.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginpostinglist.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"

/*
 * Write item pointer into leaf data page using varbyte encoding. Since
 * BlockNumber is stored in incremental manner we also need a previous item
 * pointer.
 */
CompressedPostingList
ginDataPageLeafWriteItemPointer(CompressedPostingList ptr, ItemPointer iptr, ItemPointer prev)
{
	uint32		blockNumberIncr;
	uint16		offset;
	uint8		v;

	Assert(ItemPointerGetBlockNumber(iptr) != InvalidBlockNumber);
	Assert(ItemPointerGetOffsetNumber(iptr) != InvalidOffsetNumber);
	Assert(ginCompareItemPointers(iptr, prev) > 0);
	blockNumberIncr = iptr->ip_blkid.bi_lo + (iptr->ip_blkid.bi_hi << 16) -
					  (prev->ip_blkid.bi_lo + (prev->ip_blkid.bi_hi << 16));
	for (;;)
	{
		if (blockNumberIncr < HIGHBIT)
		{
			v = (uint8) blockNumberIncr;
			*ptr = v;
			ptr++;
			break;
		}
		else
		{
			v = ((uint8) blockNumberIncr) | HIGHBIT;
			*ptr = v;
			ptr++;
			blockNumberIncr >>= 7;
		}
	}

	offset = iptr->ip_posid;
	for (;;)
	{
		if (offset < HIGHBIT)
		{
			v = (uint8) offset;
			*ptr = v;
			ptr++;
			break;
		}
		else
		{
			v = ((uint8) offset) | HIGHBIT;
			*ptr = v;
			ptr++;
			offset >>= 7;
		}
	}

	return ptr;
}

/*
 * Calculate size of incremental varbyte encoding of item pointer.
 */
int
ginDataPageLeafGetItemPointerSize(ItemPointer iptr, ItemPointer prev)
{
	uint32		blockNumberIncr;
	uint16		offset;
	int			size = 0;

	Assert(ginCompareItemPointers(iptr, prev) > 0);

	blockNumberIncr = iptr->ip_blkid.bi_lo + (iptr->ip_blkid.bi_hi << 16) -
					  (prev->ip_blkid.bi_lo + (prev->ip_blkid.bi_hi << 16));
	do
	{
		size++;
		blockNumberIncr >>= 7;
	} while (blockNumberIncr > 0);

	offset = iptr->ip_posid;
	do
	{
		size++;
		offset >>= 7;
	} while (offset > 0);

	return size;
}


/*
 * Merge two ordered arrays of itempointers, eliminating any duplicates.
 * Returns the number of items in the result.
 * Caller is responsible that there is enough space at *dst.
 */
uint32
ginMergeItemPointers(ItemPointerData *dst,
					 ItemPointerData *a, uint32 na,
					 ItemPointerData *b, uint32 nb)
{
	ItemPointerData *dptr = dst;
	ItemPointerData *aptr = a,
			   *bptr = b;

	while (aptr - a < na && bptr - b < nb)
	{
		int			cmp = ginCompareItemPointers(aptr, bptr);

		if (cmp > 0)
			*dptr++ = *bptr++;
		else if (cmp == 0)
		{
			/* we want only one copy of the identical items */
			*dptr++ = *bptr++;
			aptr++;
		}
		else
			*dptr++ = *aptr++;
	}

	while (aptr - a < na)
		*dptr++ = *aptr++;

	while (bptr - b < nb)
		*dptr++ = *bptr++;

	return dptr - dst;
}
