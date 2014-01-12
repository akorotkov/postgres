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

#define CHECK_ENCODING_ROUNDTRIP


/*
 * For encoding purposes, item pointers are represented as 64-bit unsigned
 * integers. The lowest 11 bits represent the offset number, and the next
 * lowest 32 bits are the block number. That leaves 17 bits unused, ie.
 * only 43 low bits are used.
 *
 * These 43-bit integers are encoded using varbyte encoding. 43 bits fit
 * conveniently in exactly 6 bytes when varbyte encoded:
 *
 * 0XXXXXXX											         7  0
 * 1XXXXXXX 0XXXXYYY										11  3
 * 1XXXXXXX 1XXXXYYY 0YYYYYYY								11 10
 * 1XXXXXXX 1XXXXYYY 1YYYYYYY 0YYYYYYY						11 17
 * 1XXXXXXX 1XXXXYYY 1YYYYYYY 1YYYYYYY 0YYYYYYY				11 24
 * 1XXXXXXX 1XXXXYYY 1YYYYYYY 1YYYYYYY 1YYYYYYY YYYYYYYY	11 32
 *
 * X = bits used for offset number
 * Y = bits used for block number
 */

/*
 * How many bits do you need to encode offset number? It's a 16-bit integer,
 * but you can't fit that many items on a page. 11 ought to be more than
 * enough. It's appealing to derive this from MaxHeapTuplesPerPage, and use
 * the minimum number of bits, but that would change the on-disk format if
 * MaxHeapTuplesPerPage changes.
 */
#define MaxHeapTuplesPerPageBits		11

static inline uint64
itemptr_to_uint64(const ItemPointer iptr)
{
	uint64 val;

	Assert(ItemPointerIsValid(iptr));

	val = iptr->ip_blkid.bi_hi;
	val <<= 16;
	val |= iptr->ip_blkid.bi_lo;
	val <<= MaxHeapTuplesPerPageBits;
	val |= iptr->ip_posid;

	return val;
}

static inline void
uint64_to_itemptr(uint64 val, ItemPointer iptr)
{
	iptr->ip_posid = val & ((1 << MaxHeapTuplesPerPageBits) - 1);
	val = val >> MaxHeapTuplesPerPageBits;
	iptr->ip_blkid.bi_lo = val & 0xFFFF;
	val = val >> 16;
	iptr->ip_blkid.bi_hi = val & 0xFFFF;

	Assert(ItemPointerIsValid(iptr));
}

/*
 * Varbyte-encode 'val' into *ptr.
 */
static void
encode_word(uint64 val, unsigned char **ptr)
{
	unsigned char *p = *ptr;

	while (val > 0x7F)
	{
		*(p++) = 0x80 | (val & 0x7F);
		val >>= 7;
	}
	*(p++) = (unsigned char) val;

	*ptr = p;
}

/*
 * Decode *ptr.
 */
static uint64
decode_word(unsigned char **ptr)
{
	uint64 val;
	unsigned char *p = *ptr;
	uint64 c;

	c = *(p++);
	val = c & 0x7F;
	if (c & 0x80)
	{
		c = *(p++);
		val |= (c & 0x7F) << 7;
		if (c & 0x80)
		{
			c = *(p++);
			val |= (c & 0x7F) << 14;
			if (c & 0x80)
			{
				c = *(p++);
				val |= (c & 0x7F) << 21;
				if (c & 0x80)
				{
					c = *(p++);
					val |= (c & 0x7F) << 28;
					if (c & 0x80)
					{
						c = *(p++);
						val |= (c & 0x7F) << 35;
						if (c & 0x80)
						{
							c = *(p++);
							val |= c << 42;
						}
					}
				}
			}
		}
	}

#ifdef CODEC_DEBUG
	elog(LOG, "decoded %ld bytes to %u/%u", p - *ptr, blkdelta, offdelta);
#endif

	*ptr = p;

	return val;
}

/*
 * Encode a posting list.
 *
 * The encoded list is returned in a palloc'd segment, which will be at
 * most 'maxsize' bytes in size.  The number items in the returned segment
 * is returned in *nwritten. If it's not equal to nipd, not all the items
 * fit in 'maxsize', and only the first *nwritten were encoded.
 */
#define WORD_BITS 16
PostingListSegment *
ginCompressPostingList(const ItemPointer ipd, int nipd, int maxsize,
					   int *nwritten)
{
	uint64		prev;
	int			totalpacked = 0;
	int			maxbytes;
	PostingListSegment *seg;
	unsigned char	   *ptr;
	unsigned char	   *endptr;

	seg = palloc(maxsize);

	maxbytes = maxsize - offsetof(PostingListSegment, bytes);

	/* Store the first special item */
	seg->first = ipd[0];

	prev = itemptr_to_uint64(&seg->first);

	ptr = seg->bytes;
	endptr = seg->bytes + maxbytes;
	for (totalpacked = 1; totalpacked < nipd; totalpacked++)
	{
		uint64		val = itemptr_to_uint64(&ipd[totalpacked]);
		uint64		delta = val - prev;

		if (endptr - ptr >= 6)
			encode_word(delta, &ptr);
		else
		{
			/*
			 * There are less than 6 bytes left. Have to check if the next
			 * item fits in that space before writing it out.
			 */
			unsigned char buf[6];
			unsigned char *p = buf;

			encode_word(delta, &p);
			if (p - buf > (endptr - ptr))
				break; /* output is full */

			memcpy(ptr, buf, p - buf);
			ptr += (p - buf);
		}
		prev = val;
	}
	seg->nbytes = ptr - seg->bytes;

	if (nwritten)
		*nwritten = totalpacked;

	Assert(SizeOfPostingListSegment(seg) <= maxsize);

#if defined (CHECK_ENCODING_ROUNDTRIP) && defined(USE_ASSERT_CHECKING)
	/*
	 * Check that the encoded segment decodes back to the original items
	 */
	if (assert_enabled)
	{
		int ndecoded;
		ItemPointer tmp = ginPostingListDecodeSegment(seg, &ndecoded);
		int i;

		Assert(ndecoded == totalpacked);
		for (i = 0; i < ndecoded; i++)
			Assert(memcmp(&tmp[i], &ipd[i], sizeof(ItemPointerData)) == 0);
	}
#endif

	return seg;
}

/*
 * Decode a single posting list segment into an array of item pointers.
 * The number of items is returned in *ndecoded.
 */
ItemPointer
ginPostingListDecodeSegment(PostingListSegment *segment, int *ndecoded)
{
	return ginPostingListDecodeAllSegments(segment,
										   SizeOfPostingListSegment(segment),
										   ndecoded);
}

/*
 * Decode multiple posting list segments into an array of item pointers.
 * The number of items is returned in *ndecoded_out. The segments are stored
 * one after each other, with total size 'len' bytes.
 */
ItemPointer
ginPostingListDecodeAllSegments(PostingListSegment *segment, int len,
								int *ndecoded_out)
{
	ItemPointer	result;
	int			nallocated;
	uint64		val;
	char	   *endseg = ((char *) segment) + len;
	int			ndecoded;
	unsigned char *ptr;
	unsigned char *endptr;

	nallocated = segment->nbytes + 5;
	result = palloc(nallocated * sizeof(ItemPointerData));

	ndecoded = 0;
	while ((char *) segment < endseg)
	{
		/* enlarge output array if needed */
		if (ndecoded >= nallocated)
		{
			nallocated *= 2;
			result = repalloc(result, nallocated * sizeof(ItemPointerData));
		}

		/* copy the first item */
		result[ndecoded] = segment->first;
		ndecoded++;
		Assert(OffsetNumberIsValid(ItemPointerGetOffsetNumber(&segment->first)));

		val = itemptr_to_uint64(&segment->first);
		ptr = segment->bytes;
		endptr = segment->bytes + segment->nbytes;
		while (ptr < endptr)
		{
			/* enlarge output array if needed */
			if (ndecoded >= nallocated)
			{
				nallocated *= 2;
				result = repalloc(result, nallocated * sizeof(ItemPointerData));
			}

			val += decode_word(&ptr);

			uint64_to_itemptr(val, &result[ndecoded]);
			ndecoded++;
		}
		segment = NextPostingListSegment(segment);
	}

	if (ndecoded_out)
		*ndecoded_out = ndecoded;
	return result;
}

/*
 * Add all item pointers from a bunch of posting lists to a TIDBitmap.
 */
int
ginPostingListDecodeToTbm(PostingListSegment *segment, int len,
						  TIDBitmap *tbm)
{
	int			ndecoded;
	ItemPointer	items;

	items = ginPostingListDecodeAllSegments(segment, len, &ndecoded);
	tbm_add_tuples(tbm, items, ndecoded, false);
	pfree(items);

	return ndecoded;
}

/*
 * Merge two ordered arrays of itempointers, eliminating any duplicates.
 * Returns the number of items in the result.
 * Caller is responsible that there is enough space at *dst.
 *
 * It's OK if 'dst' overlaps with the *beginning* of one of the arguments.
 */
uint32
ginMergeItemPointers(ItemPointerData *dst,
					 ItemPointerData *a, uint32 na,
					 ItemPointerData *b, uint32 nb)
{
	ItemPointerData *dptr = dst;
	ItemPointerData *aptr = a,
			   *bptr = b;

	/* The inputs should be in order */
#ifdef USE_ASSERT_CHECKING
	if (assert_enabled)
	{
		int i;

		for (i = 1; i < na; i++)
		{
			Assert(ginCompareItemPointers(&a[i - 1], &a[i]) < 0);
		}
		for (i = 1; i < nb; i++)
		{
			Assert(ginCompareItemPointers(&b[i - 1], &b[i]) < 0);
		}
	}
#endif

	/*
	 * If the argument arrays don't overlap, we can just append them to
	 * each other.
	 */
	if (na == 0 || nb == 0 || ginCompareItemPointers(&a[na - 1], &b[0]) < 0)
	{
		memmove(dst, a, na * sizeof(ItemPointerData));
		memmove(&dst[na], b, nb * sizeof(ItemPointerData));
		return na + nb;
	}
	else if (ginCompareItemPointers(&b[nb - 1], &a[0]) < 0)
	{
		memmove(dst, b, nb * sizeof(ItemPointerData));
		memmove(&dst[nb], a, na * sizeof(ItemPointerData));
		return na + nb;
	}

	while (aptr - a < na && bptr - b < nb)
	{
		int			cmp = ginCompareItemPointers(aptr, bptr);

		Assert(ItemPointerGetOffsetNumber(aptr) < 1000);
		Assert(ItemPointerGetOffsetNumber(bptr) < 1000);

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
