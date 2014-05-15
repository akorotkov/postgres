/*-------------------------------------------------------------------------
 *
 * vodkajsonbentryproc.c
 *	  implementation of radix tree (compressed trie) over text
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkajsonbentryproc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/spgist.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"

#define VodkaMatchStrategyNumber 99

/*
 * In the worst case, an inner tuple in a text radix tree could have as many
 * as 256 nodes (one for each possible byte value).  Each node can take 16
 * bytes on MAXALIGN=8 machines.  The inner tuple must fit on an index page
 * of size BLCKSZ.	Rather than assuming we know the exact amount of overhead
 * imposed by page headers, tuple headers, etc, we leave 100 bytes for that
 * (the actual overhead should be no more than 56 bytes at this writing, so
 * there is slop in this number).  So we can safely create prefixes up to
 * BLCKSZ - 256 * 16 - 100 bytes long.	Unfortunately, because 256 * 16 is
 * already 4K, there is no safe prefix length when BLCKSZ is less than 8K;
 * it is always possible to get "SPGiST inner tuple size exceeds maximum"
 * if there are too many distinct next-byte values at a given place in the
 * tree.  Since use of nonstandard block sizes appears to be negligible in
 * the field, we just live with that fact for now, choosing a max prefix
 * size of 32 bytes when BLCKSZ is configured smaller than default.
 */
#define SPGIST_MAX_PREFIX_LENGTH	Max((int) (BLCKSZ - 256 * 16 - 100), 32)

/* Struct for sorting values in picksplit */
typedef struct spgNodePtr
{
	Datum		d;
	int			i;
	uint16		c;
} spgNodePtr;


Datum
spg_bytea_config(PG_FUNCTION_ARGS)
{
	/* spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0); */
	spgConfigOut *cfg = (spgConfigOut *) PG_GETARG_POINTER(1);

	cfg->prefixType = BYTEAOID;
	cfg->labelType = INT2OID;
	cfg->canReturnData = true;
	cfg->longValuesOK = true;	/* suffixing will shorten long values */
	PG_RETURN_VOID();
}

/*
 * Form a text datum from the given not-necessarily-null-terminated string,
 * using short varlena header format if possible
 */
static Datum
formByteaDatum(const char *data, int datalen)
{
	char	   *p;

	p = (char *) palloc(datalen + VARHDRSZ);

	if (datalen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
	{
		SET_VARSIZE_SHORT(p, datalen + VARHDRSZ_SHORT);
		if (datalen)
			memcpy(p + VARHDRSZ_SHORT, data, datalen);
	}
	else
	{
		SET_VARSIZE(p, datalen + VARHDRSZ);
		memcpy(p + VARHDRSZ, data, datalen);
	}

	return PointerGetDatum(p);
}

/*
 * Find the length of the common prefix of a and b
 */
static int
commonPrefix(const char *a, const char *b, int lena, int lenb)
{
	int			i = 0;

	while (i < lena && i < lenb && *a == *b)
	{
		a++;
		b++;
		i++;
	}

	return i;
}

/*
 * Binary search an array of uint16 datums for a match to c
 *
 * On success, *i gets the match location; on failure, it gets where to insert
 */
static bool
searchChar(Datum *nodeLabels, int nNodes, uint16 c, int *i)
{
	int			StopLow = 0,
				StopHigh = nNodes;

	while (StopLow < StopHigh)
	{
		int			StopMiddle = (StopLow + StopHigh) >> 1;
		uint16		middle = DatumGetUInt16(nodeLabels[StopMiddle]);

		if (c < middle)
			StopHigh = StopMiddle;
		else if (c > middle)
			StopLow = StopMiddle + 1;
		else
		{
			*i = StopMiddle;
			return true;
		}
	}

	*i = StopHigh;
	return false;
}

#define JSONB_VODKA_FLAG_VALUE		0x01

#define JSONB_VODKA_FLAG_NULL		0x00
#define JSONB_VODKA_FLAG_STRING		0x02
#define JSONB_VODKA_FLAG_NUMERIC	0x04
#define JSONB_VODKA_FLAG_BOOL		0x06
#define JSONB_VODKA_FLAG_TYPE		0x06
#define JSONB_VODKA_FLAG_TRUE		0x08
#define JSONB_VODKA_FLAG_NAN		0x08
#define JSONB_VODKA_FLAG_NEGATIVE	0x10

#define JSONB_VODKA_FLAG_ARRAY		0x02

typedef enum
{
	sInitial = 0,
	sInKey = 1,
	sInValue = 2,
	sInNumeric = 3
} ChooseStatus;

#define GET_LEVEL_LEN(level) ((level) / 4)
#define GET_LEVEL_STATUS(level) ((ChooseStatus)((level) % 4))
#define LEVEL_ADD(prev,status,len) ((len) * 4 + (int)(status) - (prev))

static ChooseStatus
getNextStatus(ChooseStatus status, char c)
{
	switch (status)
	{
		case sInitial:
			if (c & JSONB_VODKA_FLAG_VALUE)
			{
				if ((c & JSONB_VODKA_FLAG_TYPE) == JSONB_VODKA_FLAG_NUMERIC)
					return sInNumeric;
				else
					return sInValue;
			}
			else
			{
				if (c & JSONB_VODKA_FLAG_ARRAY)
					return sInitial;
				else
					return sInKey;
			}
			break;
		case sInKey:
			if (c == '\0')
				return sInitial;
			else
				return sInKey;
		case sInValue:
			return sInValue;
		case sInNumeric:
			return sInNumeric;
	}
}

Datum
spg_bytea_choose(PG_FUNCTION_ARGS)
{
	spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
	spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
	bytea	   *inText = DatumGetByteaPP(in->datum);
	char	   *inStr = VARDATA_ANY(inText);
	int			inSize = VARSIZE_ANY_EXHDR(inText);
	uint16		nodeChar = 0x100;
	int			i = 0;
	int			commonLen = 0;
	int			len;
	ChooseStatus status;

	len = GET_LEVEL_LEN(in->level);
	status = GET_LEVEL_STATUS(in->level);

	if (status == sInNumeric)
	{
		Numeric value = DatumGetNumeric(in->leafDatum);
		Numeric middle = DatumGetNumeric(in->prefixDatum);
		int	cmp;

		Assert(in->nNodes == 2);

		cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
				NumericGetDatum(value),
				NumericGetDatum(middle)));

		out->resultType = spgMatchNode;
		out->result.matchNode.nodeN = (cmp < 0) ? 0 : 1;
		out->result.matchNode.levelAdd = LEVEL_ADD(in->level, status, len + 1);
		out->result.matchNode.restDatum = in->leafDatum;
	}
	else
	{
		/* Check for prefix match, set nodeChar to first byte after prefix */
		if (in->hasPrefix)
		{
			bytea	   *prefixText = DatumGetByteaPP(in->prefixDatum);
			char	   *prefixStr = VARDATA_ANY(prefixText);
			int			prefixSize = VARSIZE_ANY_EXHDR(prefixText);

			commonLen = commonPrefix(inStr + len,
									 prefixStr,
									 inSize - len,
									 prefixSize);

			for (i = 0 ; i < commonLen; i++)
			{
				status = getNextStatus(status, prefixStr[i]);
				Assert(status != sInNumeric);
			}

			if (commonLen == prefixSize)
			{
				if (inSize - len > commonLen)
				{
					nodeChar = *(uint8 *) (inStr + len + commonLen);
					status = getNextStatus(status, (char)nodeChar);
				}
				else
				{
					nodeChar = 0x100;
				}
			}
			else
			{
				/* Must split tuple because incoming value doesn't match prefix */
				out->resultType = spgSplitTuple;

				if (commonLen == 0)
				{
					out->result.splitTuple.prefixHasPrefix = false;
				}
				else
				{
					out->result.splitTuple.prefixHasPrefix = true;
					out->result.splitTuple.prefixPrefixDatum =
						formByteaDatum(prefixStr, commonLen);
				}
				out->result.splitTuple.nodeLabel =
					UInt16GetDatum(*(prefixStr + commonLen));

				if (prefixSize - commonLen == 1)
				{
					out->result.splitTuple.postfixHasPrefix = false;
				}
				else
				{
					out->result.splitTuple.postfixHasPrefix = true;
					out->result.splitTuple.postfixPrefixDatum =
						formByteaDatum(prefixStr + commonLen + 1,
									  prefixSize - commonLen - 1);
				}

				PG_RETURN_VOID();
			}
		}
		else if (inSize > len)
		{
			nodeChar = *(uint8 *) (inStr + len);
		}
		else
		{
			nodeChar = 0x100;
		}

		/* Look up nodeChar in the node label array */
		if (searchChar(in->nodeLabels, in->nNodes, nodeChar, &i))
		{
			/*
			 * Descend to existing node.  (If in->allTheSame, the core code will
			 * ignore our nodeN specification here, but that's OK.  We still have
			 * to provide the correct levelAdd and restDatum values, and those are
			 * the same regardless of which node gets chosen by core.)
			 */
			out->resultType = spgMatchNode;
			out->result.matchNode.nodeN = i;
			out->result.matchNode.levelAdd = LEVEL_ADD(in->level, status, len + commonLen + 1);
			if (status == sInNumeric)
			{
				out->result.matchNode.restDatum =
					PointerGetDatum(inStr + len + commonLen + 1);
			}
			else
			{
				if (inSize - len - commonLen - 1 > 0)
					out->result.matchNode.restDatum =
						formByteaDatum(inStr + len + commonLen + 1,
									  inSize - len - commonLen - 1);
				else
					out->result.matchNode.restDatum =
						formByteaDatum(NULL, 0);
			}
		}
		else if (in->allTheSame)
		{
			/*
			 * Can't use AddNode action, so split the tuple.  The upper tuple has
			 * the same prefix as before and uses an empty node label for the
			 * lower tuple.  The lower tuple has no prefix and the same node
			 * labels as the original tuple.
			 */
			out->resultType = spgSplitTuple;
			out->result.splitTuple.prefixHasPrefix = in->hasPrefix;
			out->result.splitTuple.prefixPrefixDatum = in->prefixDatum;
			out->result.splitTuple.nodeLabel = UInt16GetDatum(0x100);
			out->result.splitTuple.postfixHasPrefix = false;
		}
		else
		{
			/* Add a node for the not-previously-seen nodeChar value */
			out->resultType = spgAddNode;
			out->result.addNode.nodeLabel = UInt16GetDatum(nodeChar);
			out->result.addNode.nodeN = i;
		}
	}

	PG_RETURN_VOID();
}

/* qsort comparator to sort spgNodePtr structs by "c" */
static int
cmpNodePtr(const void *a, const void *b)
{
	const spgNodePtr *aa = (const spgNodePtr *) a;
	const spgNodePtr *bb = (const spgNodePtr *) b;

	if (aa->c == bb->c)
		return 0;
	else if (aa->c > bb->c)
		return 1;
	else
		return -1;
}

typedef struct SortedNumeric
{
	Numeric	    n;
	int			i;
} SortedNumeric;

static int
cmpNumeric(const void *a, const void *b)
{
	SortedNumeric *na = (SortedNumeric *) a;
	SortedNumeric *nb = (SortedNumeric *) b;

	return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
			 NumericGetDatum(na->n),
			 NumericGetDatum(nb->n)));
}

Datum
spg_bytea_picksplit(PG_FUNCTION_ARGS)
{
	spgPickSplitIn *in = (spgPickSplitIn *) PG_GETARG_POINTER(0);
	spgPickSplitOut *out = (spgPickSplitOut *) PG_GETARG_POINTER(1);
	bytea	   *text0 = DatumGetByteaPP(in->datums[0]);
	int			i,
				len,
				commonLen;
	spgNodePtr *nodes;
	ChooseStatus status;

	len = GET_LEVEL_LEN(in->level);
	status = GET_LEVEL_STATUS(in->level);

	if (status == sInNumeric)
	{
		SortedNumeric *sorted;
		int			middle;
		Numeric		middleValue;
		int			size = 0;

		sorted = palloc(sizeof(*sorted) * in->nTuples);
		for (i = 0; i < in->nTuples; i++)
		{
			sorted[i].n = DatumGetNumeric(in->datums[i]);
			sorted[i].i = i;
			size += VARSIZE_ANY(sorted[i].n);
		}

		qsort(sorted, in->nTuples, sizeof(*sorted), cmpNumeric);
		middle = in->nTuples >> 1;
		middleValue = sorted[middle].n;

		out->hasPrefix = true;
		out->prefixDatum = NumericGetDatum(middleValue);

		out->nNodes = 2;
		out->nodeLabels = NULL;		/* we don't need node labels */

		out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
		out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

		/*
		 * Note: points that have coordinates exactly equal to coord may get
		 * classified into either node, depending on where they happen to fall in
		 * the sorted list.  This is okay as long as the inner_consistent function
		 * descends into both sides for such cases.  This is better than the
		 * alternative of trying to have an exact boundary, because it keeps the
		 * tree balanced even when we have many instances of the same point value.
		 * So we should never trigger the allTheSame logic.
		 */
		size = 0;
		for (i = 0; i < in->nTuples; i++)
		{
			Numeric	    value = sorted[i].n;
			int			n = sorted[i].i;

			out->mapTuplesToNodes[n] = (i < middle) ? 0 : 1;
			out->leafTupleDatums[n] = in->datums[n];
			size += VARSIZE_ANY(value);
		}
	}
	else
	{
		/* Identify longest common prefix, if any */
		commonLen = VARSIZE_ANY_EXHDR(text0);
		for (i = 1; i < in->nTuples && commonLen > 0; i++)
		{
			bytea	   *texti = DatumGetByteaPP(in->datums[i]);
			int			tmp = commonPrefix(VARDATA_ANY(text0),
										   VARDATA_ANY(texti),
										   VARSIZE_ANY_EXHDR(text0),
										   VARSIZE_ANY_EXHDR(texti));

			if (tmp < commonLen)
				commonLen = tmp;
		}

		/*
		 * Limit the prefix length, if necessary, to ensure that the resulting
		 * inner tuple will fit on a page.
		 */
		commonLen = Min(commonLen, SPGIST_MAX_PREFIX_LENGTH);
		i = 0;
		while (i < commonLen)
		{
			status = getNextStatus(status, VARDATA_ANY(text0)[i]);
			if (status == sInNumeric || status == sInValue)
				break;
			i++;
		}
		commonLen = i;

		/* Set node prefix to be that string, if it's not empty */
		if (commonLen == 0)
		{
			out->hasPrefix = false;
		}
		else
		{
			out->hasPrefix = true;
			out->prefixDatum = formByteaDatum(VARDATA_ANY(text0), commonLen);
		}

		/* Extract the node label (first non-common byte) from each value */
		nodes = (spgNodePtr *) palloc(sizeof(spgNodePtr) * in->nTuples);

		for (i = 0; i < in->nTuples; i++)
		{
			bytea	   *texti = DatumGetByteaPP(in->datums[i]);

			if (commonLen < VARSIZE_ANY_EXHDR(texti))
				nodes[i].c = *(uint8 *) (VARDATA_ANY(texti) + commonLen);
			else
				nodes[i].c = 0x100;	/* use \0 if string is all common */
			nodes[i].i = i;
			nodes[i].d = in->datums[i];
		}

		/*
		 * Sort by label bytes so that we can group the values into nodes.	This
		 * also ensures that the nodes are ordered by label value, allowing the
		 * use of binary search in searchChar.
		 */
		qsort(nodes, in->nTuples, sizeof(*nodes), cmpNodePtr);

		/* And emit results */
		out->nNodes = 0;
		out->nodeLabels = (Datum *) palloc(sizeof(Datum) * in->nTuples);
		out->mapTuplesToNodes = (int *) palloc(sizeof(int) * in->nTuples);
		out->leafTupleDatums = (Datum *) palloc(sizeof(Datum) * in->nTuples);

		for (i = 0; i < in->nTuples; i++)
		{
			bytea	   *texti = DatumGetByteaPP(nodes[i].d);
			Datum		leafD;

			if (i == 0 || nodes[i].c != nodes[i - 1].c)
			{
				out->nodeLabels[out->nNodes] = UInt16GetDatum(nodes[i].c);
				out->nNodes++;
			}

			if (commonLen < VARSIZE_ANY_EXHDR(texti))
				leafD = formByteaDatum(VARDATA_ANY(texti) + commonLen + 1,
									  VARSIZE_ANY_EXHDR(texti) - commonLen - 1);
			else
				leafD = formByteaDatum(NULL, 0);

			out->leafTupleDatums[nodes[i].i] = leafD;
			out->mapTuplesToNodes[nodes[i].i] = out->nNodes - 1;
		}
	}

	PG_RETURN_VOID();
}

typedef enum
{
	iAny = 1,
	iAnyArray,
	iKey,
	iAnyKey
} PathItemType;

typedef struct PathItem PathItem;
struct PathItem
{
	char		   *s;
	PathItemType	type;
	int				len;
	PathItem	   *parent;
};

typedef struct
{
	uint8	type;
	uint32	hash;
	Numeric	n;
} JsonbVodkaValue;

typedef struct
{
	int	pathLength;
	PathItem *path;
	JsonbVodkaValue *exact, *leftBound, *rightBound;
	bool inequality, leftInclusive, rightInclusive;
} JsonbVodkaKey;

static bool
cmpParts(char *s1, int len1, char *s2part1, int len2part1,
		 char *s2part2, int len2part2)
{
	if (len1 != len2part1 + len2part2)
		return false;
	if (memcmp(s1, s2part1, len2part1))
		return false;
	if (memcmp(s1 + len2part1, s2part2, len2part2))
		return false;
	return true;
}

static Datum
processPrefix(ChooseStatus status, JsonbVodkaKey *key, char *prefix, int len,
	Datum reconstrValue)
{
	int i, start = -1, j, reconstrLen;
	bool *flags;
	char *reconstr;

	if (DatumGetPointer(reconstrValue))
	{
		flags = (bool *)palloc(sizeof(bool) * (key->pathLength + 1));
		memcpy(flags, VARDATA_ANY(DatumGetPointer(reconstrValue)), sizeof(bool) * (key->pathLength + 1));
		reconstr = VARDATA_ANY(DatumGetPointer(reconstrValue)) + key->pathLength + 1;
		reconstrLen = VARSIZE_ANY_EXHDR(reconstrValue) - (key->pathLength + 1);
	}
	else
	{
		flags = (bool *)palloc0(sizeof(bool) * (key->pathLength + 1));
		flags[0] = true;
		reconstr = NULL;
		reconstrLen = 0;
	}

	for (i = 0; i < len; i++)
	{
		char c = prefix[i];
		switch (status)
		{
			case sInitial:
				Assert(!(c & JSONB_VODKA_FLAG_VALUE));
				if (c & JSONB_VODKA_FLAG_ARRAY)
				{
					bool prev, next = false, allfalse = true;
					for (j = 0; j < key->pathLength; j++)
					{
						prev = next;
						switch (key->path[j].type)
						{
							case iAny:
								next = flags[j];
								flags[j] = prev || flags[j];
								break;
							case iAnyKey:
								next = false;
								flags[j] = prev;
								break;
							case iKey:
								next = false;
								flags[j] = prev;
								break;
							case iAnyArray:
								next = flags[j];
								flags[j] = prev;
								break;
						}
						if (flags[j])
							allfalse = false;
					}
					if (allfalse && !next)
						return (Datum)0;
					flags[key->pathLength - 1] = next;

					start = i + 1;
					status = sInitial;
				}
				else
				{
					start = i + 1;
					status = sInKey;
				}
				break;
			case sInKey:
				if (c == '\0')
				{
					bool prev, next = false, allfalse = true;
					for (j = 0; j < key->pathLength; j++)
					{
						prev = next;
						switch (key->path[j].type)
						{
							case iAny:
								next = flags[j];
								flags[j] = prev || flags[j];
								break;
							case iAnyKey:
								next = flags[j];
								flags[j] = prev;
								break;
							case iKey:
								if (flags[j])
								{
									next = false;
									if (start >= 0)
									{
										if (i - start == key->path[j].len &&
												memcmp(key->path[j].s, prefix + start, key->path[j].len) == 0)
											next = true;
									}
									else
									{
										next = cmpParts(key->path[j].s, key->path[j].len,
												reconstr, reconstrLen, prefix , i);
									}
								}
								flags[j] = prev;
								break;
							case iAnyArray:
								next = false;
								flags[j] = prev;
								break;
						}
						if (flags[j])
							allfalse = false;
					}
					if (allfalse && !next)
						return (Datum)0;
					flags[key->pathLength] = next;
					start = i + 1;
					status = sInitial;
				}
				else
				{
					status = sInKey;
				}
				break;
			case sInValue:
			case sInNumeric:
				Assert(false);
				break;
		}
	}

	if (start > 0)
	{
		bytea *result;
		int resultLen = VARHDRSZ + (key->pathLength + 1) + (len - start);

		result = (bytea *)palloc(resultLen);
		SET_VARSIZE(result, resultLen);
		memcpy(VARDATA(result), flags, key->pathLength + 1);
		memcpy(VARDATA(result) + key->pathLength + 1, prefix + start, len - start);
		return PointerGetDatum(result);
	}
	else
	{
		bytea *result;
		int resultLen = VARHDRSZ + (key->pathLength + 1) + reconstrLen + len;
		Pointer ptr;

		result = (bytea *)palloc(resultLen);
		SET_VARSIZE(result, resultLen);
		ptr = VARDATA(result);
		memcpy(ptr, flags, key->pathLength + 1);
		ptr += key->pathLength + 1;
		memcpy(ptr, reconstr, reconstrLen);
		ptr += reconstrLen;
		memcpy(ptr, prefix, len);
		return PointerGetDatum(result);
	}
}

static JsonbVodkaKey *
getKey(ScanKey keys, int nkeys)
{
	int i;
	for (i = 0; i < nkeys; i++)
	{
		if (keys[i].sk_strategy == VodkaMatchStrategyNumber)
		{
			Assert(nkeys == 1);
			return (JsonbVodkaKey *)DatumGetPointer(keys[i].sk_argument);
		}
	}
	return NULL;
}


Datum
spg_bytea_inner_consistent(PG_FUNCTION_ARGS)
{
	spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
	spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
	bool		collate_is_c = lc_collate_is_c(PG_GET_COLLATION());
	bytea	   *reconstrText = NULL;
	int			maxReconstrLen = 0;
	bytea	   *prefixText = NULL;
	char *prefixStr;
	int			prefixSize = 0;
	int			i, len;
	JsonbVodkaKey *key = getKey(in->scankeys, in->nkeys);
	ChooseStatus status;

	len = GET_LEVEL_LEN(in->level);
	status = GET_LEVEL_STATUS(in->level);

	if (status == sInValue && key)
	{
		Pointer queryVal;
		bool prefixRes;

		Assert(!key->inequality);
		if (key->exact)
		{
			Assert(key->exact->type == (JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_STRING));
			queryVal = (Pointer)&key->exact->hash + len;
			if (in->hasPrefix)
			{
				prefixText = DatumGetByteaPP(in->prefixDatum);
				prefixSize = VARSIZE_ANY_EXHDR(prefixText);
				prefixStr = VARDATA_ANY(prefixText);

				prefixRes = (memcmp(queryVal, prefixStr, prefixSize) == 0);
				len += prefixSize;
				queryVal += prefixSize;
			}
		}

		out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
		out->levelAdds = (int *) palloc(sizeof(int) * in->nNodes);
		out->reconstructedValues = (Datum *) palloc(sizeof(Datum) * in->nNodes);
		out->nNodes = 0;

		for (i = 0; (i < in->nNodes) && prefixRes; i++)
		{
			uint16 nodeChar = DatumGetUInt16(in->nodeLabels[i]);
			char c;

			if (key->exact)
			{
				Assert(nodeChar != 0x100);
				c = (char)nodeChar;

				if (c != *queryVal)
					continue;
			}

			out->nodeNumbers[out->nNodes] = i;
			out->levelAdds[out->nNodes] = LEVEL_ADD(in->level, status, len + 1);
			out->reconstructedValues[out->nNodes] = datumCopy(in->reconstructedValue, false, -1);
			out->nNodes++;
		}

	}
	else if (status == sInNumeric)
	{
		Numeric middle = DatumGetNumeric(in->prefixDatum);
		int which;
		Assert(in->hasPrefix);

		if (in->allTheSame)
			elog(ERROR, "allTheSame should not occur for k-d trees");

		Assert(in->nNodes == 2);

		/* "which" is a bitmask of children that satisfy all constraints */
		which = (1 << 1) | (1 << 2);

		if (key)
		{
			if (key->inequality)
			{
				if (key->leftBound)
				{
					int cmp;
					cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
							 NumericGetDatum(key->leftBound->n),
							 NumericGetDatum(middle)));

					if (cmp > 0 || (cmp == 0 && !key->leftInclusive))
						which &= (1 << 2);
				}
				if (key->rightBound)
				{
					int cmp;
					cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
							 NumericGetDatum(key->rightBound->n),
							 NumericGetDatum(middle)));

					if (cmp < 0 || (cmp == 0 && !key->rightInclusive))
						which &= (1 << 1);
				}
			}
			else if (key->exact)
			{
				int cmp;
				cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
						 NumericGetDatum(key->exact->n),
						 NumericGetDatum(middle)));
				if (cmp < 0)
					which &= (1 << 1);
				if (cmp > 0)
					which &= (1 << 2);
			}
		}
		else
		{
			for (i = 0; i < in->nkeys; i++)
			{
				bytea *argument = DatumGetByteaPP(in->scankeys[i].sk_argument);
				Numeric	query = (Numeric)(VARDATA_ANY(argument) + len);
				int cmp;

				switch (in->scankeys[i].sk_strategy)
				{
					case BTEqualStrategyNumber:
						cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
								 NumericGetDatum(query),
								 NumericGetDatum(middle)));
						if (cmp < 0)
							which &= (1 << 1);
						if (cmp > 0)
							which &= (1 << 2);

						break;
					default:
						elog(ERROR, "unrecognized strategy number: %d",
							 in->scankeys[i].sk_strategy);
						break;
				}

				if (which == 0)
					break;				/* no need to consider remaining conditions */
			}
		}

		/* We must descend into the children identified by which */
		out->nodeNumbers = (int *) palloc(sizeof(int) * 2);
		out->nNodes = 0;
		for (i = 1; i <= 2; i++)
		{
			if (which & (1 << i))
				out->nodeNumbers[out->nNodes++] = i - 1;
		}

		out->reconstructedValues = (Datum *) palloc(sizeof(Datum) * 2);
		out->reconstructedValues[0] = datumCopy(in->reconstructedValue, false, -1);
		out->reconstructedValues[1] = datumCopy(in->reconstructedValue, false, -1);

		/* Set up level increments, too */
		out->levelAdds = (int *) palloc(sizeof(int) * 2);
		out->levelAdds[0] = 0;
		out->levelAdds[1] = 0;
	}
	else
	{
		if (key)
		{
			Datum reconstructedValue = in->reconstructedValue;

			if (in->hasPrefix)
			{
				prefixText = DatumGetByteaPP(in->prefixDatum);
				prefixSize = VARSIZE_ANY_EXHDR(prefixText);
				prefixStr = VARDATA_ANY(prefixText);
				maxReconstrLen += prefixSize;
				reconstructedValue = processPrefix(status, key,
						prefixStr, prefixSize, reconstructedValue);
				len += prefixSize;
				for (i = 0; i < prefixSize; i++)
				{
					status = getNextStatus(status, prefixStr[i]);
					Assert(status != sInNumeric);
				}
			}

			out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
			out->levelAdds = (int *) palloc(sizeof(int) * in->nNodes);
			out->reconstructedValues = (Datum *) palloc(sizeof(Datum) * in->nNodes);
			out->nNodes = 0;

			for (i = 0; (i < in->nNodes) && reconstructedValue; i++)
			{
				bool res;
				uint16		nodeChar = DatumGetUInt16(in->nodeLabels[i]);
				Datum nodeReconstructedValue;
				int nodeLen = len;
				ChooseStatus nodeStatus;

				if (nodeChar == 0x100)
				{
					Assert(false);
					res = false;
				}
				else if (status == sInitial && (nodeChar & JSONB_VODKA_FLAG_VALUE))
				{
					bool *flags = VARDATA_ANY(DatumGetPointer(reconstructedValue));
					nodeLen = 0;
					if (flags[key->pathLength])
					{
						if (key->inequality)
						{
							res = ((JSONB_VODKA_FLAG_TYPE & nodeChar) == JSONB_VODKA_FLAG_NUMERIC);
						}
						else if (!key->exact)
						{
							res = true;
						}
						else
						{
							res = (nodeChar == key->exact->type);
						}
					}
					else
					{
						res = false;
					}
					nodeStatus = getNextStatus(status, (char)nodeChar);
					nodeReconstructedValue = datumCopy(reconstructedValue,
							false, -1);
				}
				else
				{
					char c = (char)nodeChar;
					nodeStatus = getNextStatus(status, c);
					nodeReconstructedValue = processPrefix(status, key,
							&c, 1, reconstructedValue);
					nodeLen++;
					if (nodeReconstructedValue == (Datum)0)
						res = false;
					else
						res = true;
				}

				if (res)
				{
					out->nodeNumbers[out->nNodes] = i;
					out->levelAdds[out->nNodes] = LEVEL_ADD(in->level, nodeStatus, nodeLen);
					out->reconstructedValues[out->nNodes] = nodeReconstructedValue;
					out->nNodes++;
				}
			}
		}
		else
		{
			Assert(len == 0 ? DatumGetPointer(in->reconstructedValue) == NULL :
			VARSIZE_ANY_EXHDR(DatumGetPointer(in->reconstructedValue)) == len);

			maxReconstrLen = len + 1;
			if (in->hasPrefix)
			{
				prefixText = DatumGetByteaPP(in->prefixDatum);
				prefixSize = VARSIZE_ANY_EXHDR(prefixText);
				prefixStr = VARDATA_ANY(prefixText);
				maxReconstrLen += prefixSize;
				for (i = 0; i < prefixSize; i++)
				{
					status = getNextStatus(status, prefixStr[i]);
					Assert(status != sInNumeric);
				}
			}

			reconstrText = palloc(VARHDRSZ + maxReconstrLen);
			SET_VARSIZE(reconstrText, VARHDRSZ + maxReconstrLen);

			if (len > 0)
				memcpy(VARDATA(reconstrText),
					   VARDATA(DatumGetPointer(in->reconstructedValue)),
					   len);
			if (prefixSize)
				memcpy(((char *) VARDATA(reconstrText)) + len,
					   VARDATA_ANY(prefixText),
					   prefixSize);
			/* last byte of reconstrText will be filled in below */

			/*
			 * Scan the child nodes.  For each one, complete the reconstructed value
			 * and see if it's consistent with the query.  If so, emit an entry into
			 * the output arrays.
			 */
			out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
			out->levelAdds = (int *) palloc(sizeof(int) * in->nNodes);
			out->reconstructedValues = (Datum *) palloc(sizeof(Datum) * in->nNodes);
			out->nNodes = 0;

			for (i = 0; i < in->nNodes; i++)
			{
				uint16		nodeChar = DatumGetUInt16(in->nodeLabels[i]);
				int			thisLen;
				bool		res = true;
				int			j;

				/* If nodeChar is zero, don't include it in data */
				if (nodeChar == 0x100)
					thisLen = maxReconstrLen - 1;
				else
				{
					((char *) VARDATA(reconstrText))[maxReconstrLen - 1] = nodeChar;
					thisLen = maxReconstrLen;
				}

				for (j = 0; j < in->nkeys; j++)
				{
					StrategyNumber strategy = in->scankeys[j].sk_strategy;
					bytea	   *inText;
					int			inSize;
					int			r;

					/*
					 * If it's a collation-aware operator, but the collation is C, we
					 * can treat it as non-collation-aware.  With non-C collation we
					 * need to traverse whole tree :-( so there's no point in making
					 * any check here.
					 */
					if (strategy > 10)
					{
						if (collate_is_c)
							strategy -= 10;
						else
							continue;
					}

					inText = DatumGetByteaPP(in->scankeys[j].sk_argument);
					inSize = VARSIZE_ANY_EXHDR(inText);

					r = memcmp(VARDATA(reconstrText), VARDATA_ANY(inText),
							   Min(inSize, thisLen));

					switch (strategy)
					{
						case BTEqualStrategyNumber:
							if (r != 0 || inSize < thisLen)
								res = false;
							break;

						case VodkaMatchStrategyNumber:


						default:
							elog(ERROR, "unrecognized strategy number: %d",
								 in->scankeys[j].sk_strategy);
							break;
					}

					if (!res)
						break;			/* no need to consider remaining conditions */
				}

				if (res)
				{
					ChooseStatus nodeStatus;

					if (nodeChar == 0x100)
						nodeStatus = status;
					else
						nodeStatus = getNextStatus(status, (char)nodeChar);

					out->nodeNumbers[out->nNodes] = i;
					out->levelAdds[out->nNodes] = LEVEL_ADD(in->level, nodeStatus, thisLen);
					SET_VARSIZE(reconstrText, VARHDRSZ + thisLen);
					out->reconstructedValues[out->nNodes] =
						datumCopy(PointerGetDatum(reconstrText), false, -1);
					out->nNodes++;
				}
			}
		}
	}

	PG_RETURN_VOID();
}

static char
toHex(uint8 c)
{
	if (c <= 9)
		return '0' + c;
	else
		return 'A' + (c - 10);
}

static void
logValue(char *v, int len)
{
	char s[1024];
	int i, j = 0;

	for (i = 0; i < len; i++)
	{
		uint8 c = (uint8)v[i];
		s[j++] = toHex(c/16);
		s[j++] = toHex(c%16);
	}
	s[j++] = 0;
	elog(NOTICE, "%s", s);
}

static bool
checkNumericValue(JsonbVodkaKey *key, Numeric value)
{
	bool res = true;
	if (key->inequality)
	{
		if (key->leftBound)
		{
			if ((key->leftBound->type & JSONB_VODKA_FLAG_TYPE) != JSONB_VODKA_FLAG_NUMERIC)
			{
				res = false;
			}
			else
			{
				int cmp;
				cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
						 NumericGetDatum(value),
						 NumericGetDatum(key->leftBound->n)));

				if (cmp < 0 || (cmp == 0 && !key->leftInclusive))
					res = false;
			}
		}
		if (res && key->rightBound)
		{
			if ((key->rightBound->type & JSONB_VODKA_FLAG_TYPE) != JSONB_VODKA_FLAG_NUMERIC)
			{
				res = false;
			}
			else
			{
				int cmp;
				cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
						 NumericGetDatum(value),
						 NumericGetDatum(key->rightBound->n)));

				if (cmp > 0 || (cmp == 0 && !key->rightInclusive))
					res = false;
			}
		}
	}
	else if (key->exact)
	{
		if ((key->exact->type & JSONB_VODKA_FLAG_TYPE) != JSONB_VODKA_FLAG_NUMERIC)
		{
			res = false;
		}
		else
		{
			int cmp;
			cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
					 NumericGetDatum(value),
					 NumericGetDatum(key->exact->n)));
			if (cmp != 0)
				res = false;
		}
	}
	return res;
}

Datum
spg_bytea_leaf_consistent(PG_FUNCTION_ARGS)
{
	spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
	spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);
	bytea	   *leafValue,
			   *reconstrValue = NULL;
	char	   *fullValue;
	int			fullLen;
	bool		res;
	int			j, len;
	ChooseStatus status;
	JsonbVodkaKey *key = getKey(in->scankeys, in->nkeys);

	/* all tests are exact */
	out->recheck = false;

	len = GET_LEVEL_LEN(in->level);
	status = GET_LEVEL_STATUS(in->level);

	if (status == sInValue && key)
	{
		leafValue = DatumGetByteaPP(in->leafDatum);
		Pointer queryVal = (Pointer)&key->exact->hash + len;

		Assert(!key->inequality);
		Assert(key->exact->type == (JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_STRING));

		res = (memcmp(queryVal, VARDATA_ANY(leafValue), VARSIZE_ANY_EXHDR(leafValue)) == 0);

		out->leafValue = (Datum)0;
		out->recheck = false;
	}
	else if (status == sInNumeric)
	{
		Numeric value = DatumGetNumeric(in->leafDatum);
		bytea	   *fullBytea;
		int i;

		Assert(DatumGetPointer(in->reconstructedValue));
		reconstrValue = DatumGetByteaP(in->reconstructedValue);

		fullLen = len + VARSIZE_ANY(value);
		fullBytea = (bytea *)palloc(VARHDRSZ + fullLen);

		SET_VARSIZE(fullBytea, VARHDRSZ + fullLen);
		fullValue = VARDATA(fullBytea);
		memcpy(fullValue, VARDATA(reconstrValue), len);
		memcpy(fullValue + len, value, VARSIZE_ANY(value));
		out->leafValue = PointerGetDatum(fullBytea);

		res = true;

		if (key)
		{
			res = checkNumericValue(key, value);
		}
		else
		{
			for (i = 0; i < in->nkeys; i++)
			{
				bytea *argument = DatumGetByteaPP(in->scankeys[i].sk_argument);
				Numeric	query = (Numeric)(VARDATA_ANY(argument) + len);
				int cmp;

				switch (in->scankeys[i].sk_strategy)
				{
					case BTEqualStrategyNumber:
						cmp = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
								 NumericGetDatum(query),
								 NumericGetDatum(value)));
						if (cmp != 0)
							res = false;
						break;
					default:
						elog(ERROR, "unrecognized strategy number: %d",
							 in->scankeys[i].sk_strategy);
						break;
				}

				if (!res)
					break;
			}
		}

	}
	else
	{
		leafValue = DatumGetByteaPP(in->leafDatum);

		if (key)
		{
			char *leafStr = VARDATA_ANY(leafValue);
			int leafLen = VARSIZE_ANY_EXHDR(leafValue);
			Datum reconstructedValue;
			ChooseStatus nextStatus = status;
			bool *flags;
			int i;

			res = true;

			for (i = 0; i < leafLen; i++)
			{
				nextStatus = getNextStatus(nextStatus, leafStr[i]);
				if (nextStatus == sInNumeric || nextStatus == sInValue)
					break;
			}

			reconstructedValue = processPrefix(status, key,
					leafStr, i, in->reconstructedValue);

			if (!reconstructedValue)
			{
				res = false;
			}
			else
			{
				flags = VARDATA_ANY(DatumGetPointer(reconstructedValue));
				if (!flags[key->pathLength])
					res = false;

				if (nextStatus == sInNumeric)
				{
					res = checkNumericValue(key, (Numeric)(leafStr + i + 1));
				}
				else
				{
					if (key->inequality)
					{
						res = false;
					}
					else if (!key->exact)
					{
						res = true;
					}
					else if ((key->exact->type & JSONB_VODKA_FLAG_TYPE) != (leafStr[i] & JSONB_VODKA_FLAG_TYPE))
					{
						res = false;
					}
					else if ((key->exact->type & JSONB_VODKA_FLAG_TYPE) == JSONB_VODKA_FLAG_STRING)
					{
						Assert(leafLen == i + 5);
						res = (memcmp(leafStr + i + 1, &key->exact->hash, 4) == 0);
					}
				}
			}
		}
		else
		{
			if (DatumGetPointer(in->reconstructedValue))
				reconstrValue = DatumGetByteaP(in->reconstructedValue);

			Assert(len == 0 ? reconstrValue == NULL :
				   VARSIZE_ANY_EXHDR(reconstrValue) == len);

			/* Reconstruct the full string represented by this leaf tuple */
			fullLen = len + VARSIZE_ANY_EXHDR(leafValue);
			if (VARSIZE_ANY_EXHDR(leafValue) == 0 && len > 0)
			{
				fullValue = VARDATA(reconstrValue);
				out->leafValue = PointerGetDatum(reconstrValue);
			}
			else
			{
				bytea	   *fullText = palloc(VARHDRSZ + fullLen);

				SET_VARSIZE(fullText, VARHDRSZ + fullLen);
				fullValue = VARDATA(fullText);
				if (len)
					memcpy(fullValue, VARDATA(reconstrValue), len);
				if (VARSIZE_ANY_EXHDR(leafValue) > 0)
					memcpy(fullValue + len, VARDATA_ANY(leafValue),
						   VARSIZE_ANY_EXHDR(leafValue));
				out->leafValue = PointerGetDatum(fullText);
			}

			/* Perform the required comparison(s) */
			res = true;
			for (j = 0; j < in->nkeys; j++)
			{
				StrategyNumber strategy = in->scankeys[j].sk_strategy;
				bytea	   *query = DatumGetTextPP(in->scankeys[j].sk_argument);
				int			queryLen = VARSIZE_ANY_EXHDR(query);
				int			r;

				if (strategy > 10)
				{
					/* Collation-aware comparison */
					strategy -= 10;

					/* If asserts enabled, verify encoding of reconstructed string */
					Assert(pg_verifymbstr(fullValue, fullLen, false));

					r = varstr_cmp(fullValue, Min(queryLen, fullLen),
								   VARDATA_ANY(query), Min(queryLen, fullLen),
								   PG_GET_COLLATION());
				}
				else
				{
					/* Non-collation-aware comparison */
					r = memcmp(fullValue, VARDATA_ANY(query), Min(queryLen, fullLen));
					/*logValue(fullValue, fullLen);
					logValue(VARDATA_ANY(query), queryLen);*/
				}

				if (r == 0)
				{
					if (queryLen > fullLen)
						r = -1;
					else if (queryLen < fullLen)
						r = 1;
				}

				switch (strategy)
				{
					case BTEqualStrategyNumber:
						res = (r == 0);
						break;
					default:
						elog(ERROR, "unrecognized strategy number: %d",
							 in->scankeys[j].sk_strategy);
						res = false;
						break;
				}

				if (!res)
					break;				/* no need to consider remaining conditions */
			}
		}
	}

	PG_RETURN_BOOL(res);
}

Datum
vodka_match(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Nobody should be able to call that!");
	PG_RETURN_BOOL(false);
}

