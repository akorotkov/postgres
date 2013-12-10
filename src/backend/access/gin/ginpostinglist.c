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

#define GIN_POSTINGLIST_INCLUDE_DEFINITIONS
#include "access/gin_private.h"

/*
 * Encode a posting list.
 *
 * The encoded list is written to caller-supplied buffer 'out'. At most
 * 'maxsize' bytes are written. Returns true if the encoded items fit in
 * the reserved space, false otherwise. On true, the number of bytes
 * written is returned in *sz.
 *
 * NB: This can actually give up a few bytes short of 'maxsize', even if
 * the next item would fit. That wouldn't be hard to fix, but all the
 * current callers are happy with this behavior.
 */
bool
ginCompressPostingList(const ItemPointer ipd, int nipd,
					   int maxsize, CompressedPostingList out, Size *sz)
{
	char	   *ptr = out;
	int			i;
	uint64		writestate = 0;

	Assert(maxsize > MAX_COMPRESSED_ITEM_POINTER_SIZE);
	maxsize -= MAX_COMPRESSED_ITEM_POINTER_SIZE;

	if (nipd > 0)
	{
		for (i = 0; i < nipd && ptr - out < maxsize; i++)
			ptr = ginPostingListEncode(ptr, &ipd[i], &writestate);

		if (i < nipd)
			return false;
		*sz = ptr - out;
	}
	return true;
}

/*
 * Write item pointer into leaf data page using varbyte encoding. Since
 * BlockNumber is stored in incremental manner we also need a previous item
 * pointer.
 */
uint64
ginPostingListEncodeBegin(const ItemPointer iptr)
{
	uint64 val;

	val = iptr->ip_blkid.bi_hi;
	val <<= 16;
	val |= iptr->ip_blkid.bi_lo;
	val <<= MaxHeapTuplesPerPageBits;
	val |= iptr->ip_posid;

	return val;
}

CompressedPostingList
ginPostingListEncode(CompressedPostingList ptr,
					 const ItemPointer iptr, uint64 *state)
{
	uint64		val;
	uint8		v;
	uint64		diff;

	Assert(ItemPointerGetBlockNumber(iptr) != InvalidBlockNumber);
	Assert(ItemPointerGetOffsetNumber(iptr) != InvalidOffsetNumber);

	Assert(iptr->ip_posid < (1 << MaxHeapTuplesPerPageBits));

	val = iptr->ip_blkid.bi_hi;
	val <<= 16;
	val |= iptr->ip_blkid.bi_lo;
	val <<= MaxHeapTuplesPerPageBits;
	val |= iptr->ip_posid;

	Assert(*state < val);

	diff = val - *state;

	for (;;)
	{
		if (diff < HIGHBIT)
		{
			v = (uint8) diff;
			*ptr = v;
			ptr++;
			break;
		}
		else
		{
			v = ((uint8) diff) | HIGHBIT;
			*ptr = v;
			ptr++;
			diff >>= 7;
		}
	}

	*state = val;

	return ptr;
}

/*
 * Calculate size of incremental varbyte encoding of item pointer.
 */
int
ginPostingListPackedSize(ItemPointer iptr, ItemPointer prev)
{
	char		buf[10];
	char	   *ptr;
	uint64		state;

	state = ginPostingListEncodeBegin(prev);
	ptr = ginPostingListEncode(buf, iptr, &state);

	return ptr - buf;
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
