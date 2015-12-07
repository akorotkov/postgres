/*
 * pgduma/pgduma.c
 */
#include "postgres.h"
#include "fmgr.h"
#include "pgduma.h"
#include "c.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include <math.h>

typedef struct
{
	WordEntry  *arrb;
	WordEntry  *arre;
	char	   *values;
	char	   *operand;
} CHKVAL;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tsvector_array_match_tsquery);
Datum		tsvector_array_match_tsquery(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(tsvector_array_tsquery_distance);
Datum		tsvector_array_tsquery_distance(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(gin_extract_tsvector_array);
Datum		gin_extract_tsvector_array(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(gin_tsvector_array_consistent);
Datum		gin_tsvector_array_consistent(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(gin_tsvector_array_distance);
Datum		gin_tsvector_array_distance(PG_FUNCTION_ARGS);

#define SIXTHBIT 0x20
#define LOWERMASK 0x1F

#ifdef NOT_USED
/*
 * Get length of integer value when it's encoded using varbyte encoding.
 */
static int
getVarbyteLength(int value)
{
	int length = 0;
	while (true)
	{
		length++;
		if (value < HIGHBIT)
			break;
		value >>= 7;
	}
	return length;
}
#endif

/*
 * Write integer value in varbyte encoding.
 */
static char *
writeVarbyte(char *ptr, int value)
{
	while (true)
	{
		*ptr = (value & (~HIGHBIT)) | ((value >= HIGHBIT) ? HIGHBIT : 0);
		ptr++;
		if (value < HIGHBIT)
			break;
		value >>= 7;
	}
	return ptr;
}

/*
 * Read integer value in varbyte encoding.
 */
static char *
readVarbyte(char *ptr, int *value)
{
	uint8 v;
	int result = 0, i;

	i = 0;
	do
	{
		v = *ptr;
		ptr++;
		result |= (v & (~HIGHBIT)) << i;
		i += 7;
	}
	while (v & HIGHBIT);
	if (value)
		*value = result;
	return ptr;
}

static char *
compress_pos(char *target, uint16 pos, uint16 prev)
{
	uint16 delta;
	char *ptr;

	ptr = target;
	delta = WEP_GETPOS(pos) - WEP_GETPOS(prev);

	while (true)
	{
		if (delta >= SIXTHBIT)
		{
			*ptr = (delta & (~HIGHBIT)) | HIGHBIT;
			ptr++;
			delta >>= 7;
		}
		else
		{
			*ptr = delta | (WEP_GETWEIGHT(pos) << 5);
			ptr++;
			break;
		}
	}
	return ptr;
}

static int
compress_pos_length(uint16 pos, uint16 prev)
{
	uint16 delta;
	int length = 1;

	delta = WEP_GETPOS(pos) - WEP_GETPOS(prev);

	while (delta >= SIXTHBIT)
	{
		delta >>= 7;
		length++;
	}
	return length;
}

static char *
decompress_pos(char *ptr, uint16 *pos)
{
	int i;
	uint8 v;
	uint16 delta = 0;

	i = 0;
	while (true)
	{
		v = *ptr;
		ptr++;
		if (v & HIGHBIT)
		{
			delta |= (v & (~HIGHBIT)) << i;
		}
		else
		{
			delta |= (v & LOWERMASK) << i;
			*pos += delta;
			WEP_SETWEIGHT(*pos, v >> 5);
			return ptr;
		}
		i += 7;
	}
}

#ifdef NOT_USED
static int
count_pos(char *ptr, int len)
{
	int count = 0, i;
	for (i = 0; i < len; i++)
	{
		if (!(ptr[i] & HIGHBIT))
			count++;
	}
	return count;
}
#endif

Datum
tsvector_array_match_tsquery(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *elements;
	bool	   *isnull;
	int			nelements, i;

	deconstruct_array(array, TSVECTOROID, -1, false, 'i',
						&elements, &isnull, &nelements);

	for (i = 0; i < nelements; i++)
	{
		Datum	res;
		if (isnull[i])
			continue;
	    res = DirectFunctionCall2(ts_match_vq, elements[i], PG_GETARG_DATUM(1));
	    if (DatumGetBool(res))
	    	PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

Datum
tsvector_array_tsquery_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *elements;
	bool	   *isnull;
	int			nelements, i;
	float		result = -1.0f;

	deconstruct_array(array, TSVECTOROID, -1, false, 'i',
						&elements, &isnull, &nelements);

	for (i = 0; i < nelements; i++)
	{
		float elementResult;
		if (isnull[i])
			continue;
		elementResult = DatumGetFloat4(DirectFunctionCall2(
				ts_rank_tt, elements[i], PG_GETARG_DATUM(1)));
		if (elementResult > result)
			result = elementResult;
	}

	if (result < 0.0f)
		PG_RETURN_NULL();
	else
		PG_RETURN_FLOAT8(1.0 / (float8)result);
}

static int
findMin(Datum *elements, bool *isnull, int nelements, int *indexes, int *n, int *total_npos)
{
	int i, min_i = -1, npos;
	WordEntry  *min_we, *we;

	for (i = 0; i < nelements; i++)
	{
		TSVector vector;

		if (isnull[i])
			continue;

		vector = DatumGetTSVector(elements[i]);

		if (indexes[i] >= vector->size)
			continue;

		we = ARRPTR(vector) + indexes[i];

		if (we->haspos)
		{
			npos = _POSVECPTR(vector,we)->npos;
		}
		else
		{
			npos = 0;
		}

		if (min_i < 0)
		{
			min_i = i;
			min_we = we;
			*n = 1;
			*total_npos = npos;
		}
		else
		{
			int cmp = tsCompareString(
							STRPTR(vector) + we->pos, we->len,
							STRPTR(DatumGetTSVector(elements[min_i])) + min_we->pos, min_we->len,
							false);
			if (cmp < 0)
			{
				min_i = i;
				min_we = we;
				*n = 1;
				*total_npos = npos;
			}
			else if (cmp == 0)
			{
				(*n)++;
				*total_npos += npos;
			}
		}
	}
	return min_i;
}

static void
shift(Datum *elements, bool *isnull, int nelements, int *indexes, int shift_i)
{
	int i;
	WordEntry  *min_we, *we;

	min_we = ARRPTR(DatumGetTSVector(elements[shift_i])) + indexes[shift_i];

	for (i = 0; i < nelements; i++)
	{
		TSVector vector;

		if (i == shift_i)
			continue;
		if (isnull[i])
			continue;

		vector = DatumGetTSVector(elements[i]);

		if (indexes[i] >= vector->size)
			continue;

		we = ARRPTR(vector) + indexes[i];

		if (tsCompareString(
				STRPTR(vector) + we->pos, we->len,
				STRPTR(DatumGetTSVector(elements[shift_i])) + min_we->pos, min_we->len,
				false) == 0)
		{
			indexes[i]++;
		}
	}
	indexes[shift_i]++;
}

#define MAX_ADDINFO_SIZE 1024

typedef struct
{
	Pointer buffer;
	Pointer ptr;
	uint16 *pos;
	uint16 *posEnd;
	uint16	prev;
	int		cnt;
} ElementInfo;

Datum
gin_extract_tsvector_array(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   **addInfo = (Datum **) PG_GETARG_POINTER(3);
	bool	   **addInfoIsNull = (bool **) PG_GETARG_POINTER(4);
	Datum	   *entries = NULL;
	Datum	   *elements;
	bool	   *isnull;
	ElementInfo	*elemPos;
	int			nelements, i, totalSize, *indexes, min_i, j, n, total_npos;

	deconstruct_array(array, TSVECTOROID, -1, false, 'i',
						&elements, &isnull, &nelements);

	indexes = (int *)palloc0(sizeof(int) * nelements);
	totalSize = 0;
	for (i = 0; i < nelements; i++)
	{
		if (isnull[i])
			continue;
		totalSize += DatumGetTSVector(elements[i])->size;
	}

	elemPos = (ElementInfo *)palloc0(sizeof(ElementInfo) * nelements);
	for (i = 0; i < nelements; i++)
		elemPos[i].buffer = (Pointer)palloc0(MAX_ADDINFO_SIZE);

	entries = (Datum *) palloc(sizeof(Datum) * totalSize);
	*addInfo = (Datum *) palloc(sizeof(Datum) * totalSize);
	*addInfoIsNull = (bool *) palloc(sizeof(bool) * totalSize);
	j = 0;

	while ((min_i = findMin(elements, isnull, nelements, indexes, &n, &total_npos)) >= 0)
	{
		text	   *txt;
		WordEntry  *min_we = ARRPTR(DatumGetTSVector(elements[min_i])) + indexes[min_i];
		bytea	   *posData;
		int			posDataSize, sizeLeft = MAX_ADDINFO_SIZE - nelements, totalcnt;
		bool		flag;
		char	   *ptr;

		txt = cstring_to_text_with_len(
				STRPTR(DatumGetTSVector(elements[min_i])) + min_we->pos, min_we->len);
		entries[j] = PointerGetDatum(txt);

		posDataSize = VARHDRSZ + MAX_ADDINFO_SIZE;
		posData = (bytea *)palloc(posDataSize);
		ptr = posData->vl_dat;

		for (i = 0; i < nelements; i++)
		{
			WordEntry  *we;
			TSVector vector;

			elemPos[i].ptr = elemPos[i].buffer;
			elemPos[i].cnt = 0;
			elemPos[i].prev = 0;

			if (isnull[i])
			{
				elemPos[i].pos = NULL;
				elemPos[i].posEnd = NULL;
				continue;
			}

			vector = DatumGetTSVector(elements[i]);

			if (indexes[i] >= vector->size)
			{
				elemPos[i].pos = NULL;
				elemPos[i].posEnd = NULL;
				continue;
			}

			we = ARRPTR(vector) + indexes[i];

			if (tsCompareString(
					STRPTR(vector) + we->pos, we->len,
					STRPTR(DatumGetTSVector(elements[min_i])) + min_we->pos, min_we->len,
					false) == 0)
			{
				WordEntryPosVector *posVec;

				posVec = _POSVECPTR(vector, we);
				elemPos[i].pos = posVec->pos;
				elemPos[i].posEnd = posVec->pos + Min(127, posVec->npos);
			}
			else
			{
				elemPos[i].pos = NULL;
				elemPos[i].posEnd = NULL;
			}
		}

		flag = true;
		while (flag)
		{
			flag = false;
			for (i = 0; i < nelements; i++)
			{
				if (elemPos[i].pos < elemPos[i].posEnd)
				{
					int pos_length;
					pos_length = compress_pos_length(*(elemPos[i].pos),
							elemPos[i].prev);
					if (sizeLeft < pos_length)
						continue;
					sizeLeft -= pos_length;
					elemPos[i].ptr = compress_pos(elemPos[i].ptr,
							*(elemPos[i].pos), elemPos[i].prev);
					elemPos[i].prev = *(elemPos[i].pos);
					elemPos[i].cnt++;
					elemPos[i].pos++;
					flag = true;
				}
			}
		}
		totalcnt = 0;
		for (i = 0; i < nelements; i++)
		{
			ptr = writeVarbyte(ptr, elemPos[i].cnt);
			if (elemPos[i].cnt > 0)
			{
				Assert(elemPos[i].ptr > elemPos[i].buffer);
				Assert(elemPos[i].ptr - elemPos[i].buffer < MAX_ADDINFO_SIZE);
				memcpy(ptr, elemPos[i].buffer, (elemPos[i].ptr - elemPos[i].buffer));
				ptr += elemPos[i].ptr - elemPos[i].buffer;
				totalcnt += elemPos[i].cnt;
			}
		}
		Assert(ptr - posData->vl_dat <= MAX_ADDINFO_SIZE);
		Assert(totalcnt > 0);
		posDataSize = ptr - posData->vl_dat + VARHDRSZ;

		SET_VARSIZE(posData, posDataSize);
		(*addInfo)[j] = PointerGetDatum(posData);
		(*addInfoIsNull)[j] = false;
		j++;
		shift(elements, isnull, nelements, indexes, min_i);
	}

	*nentries = j;

	for (i = 0; i < nelements; i++)
		pfree(elemPos[i].buffer);
	pfree(elemPos);

	PG_RETURN_POINTER(entries);
}

typedef struct
{
	QueryItem  *first_item;
	bool	   *check;
	int		   *map_item_operand;
	bool	   *need_recheck;
} GinChkVal;

static bool
checkcondition_gin(void *checkval, QueryOperand *val)
{
	GinChkVal  *gcv = (GinChkVal *) checkval;
	int			j;

	/* if any val requiring a weight is used, set recheck flag */
	if (val->weight != 0)
		*(gcv->need_recheck) = true;

	/* convert item's number to corresponding entry's (operand's) number */
	j = gcv->map_item_operand[((QueryItem *) val) - gcv->first_item];

	/* return presence of current entry in indexed value */
	return gcv->check[j];
}

Datum
gin_tsvector_array_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	uint32		nentries = PG_GETARG_UINT32(3);
	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	bool		res = FALSE;

	/* The query requires recheck only if it involves weights */
	*recheck = false;

	if (query->size > 0)
	{
		QueryItem  *item;
		GinChkVal	gcv;
		int i, first_i = -1;
		char **ptrs, **endPtrs;
		bool *myCheck;

		ptrs = (char **)palloc(sizeof(char *) * nentries);
		endPtrs = (char **)palloc(sizeof(char *) * nentries);
		myCheck = (bool *)palloc0(sizeof(bool) * nentries);
		for (i = 0; i < nentries; i++)
		{
			if (check[i] && !addInfoIsNull[i])
			{
				ptrs[i] =  VARDATA_ANY(DatumGetPointer(addInfo[i]));
				endPtrs[i] = ptrs[i] + VARSIZE_ANY_EXHDR(DatumGetPointer(addInfo[i]));
				if (first_i < 0)
					first_i = i;
			}
			else
			{
				ptrs[i] = NULL;
				endPtrs[i] = NULL;
			}
		}

		gcv.first_item = item = GETQUERY(query);
		gcv.check = myCheck;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = recheck;

		while (first_i >= 0 && ptrs[first_i] < endPtrs[first_i])
		{
			for (i = 0; i < nentries; i++)
			{
				if (check[i] && !addInfoIsNull[i])
				{
					int npos;
					ptrs[i] = readVarbyte(ptrs[i], &npos);
					if (npos > 0)
					{
						uint16 pos;
						while (npos--)
						{
							ptrs[i] = decompress_pos(ptrs[i], &pos);
						}
						myCheck[i] = true;
					}
					else
					{
						myCheck[i] = false;
					}
				}
			}
			/*
			 * check-parameter array has one entry for each value (operand) in the
			 * query.
			 */

			res = TS_execute(GETQUERY(query),
							 &gcv,
							 true,
							 checkcondition_gin);
			if (res)
			{
				pfree(ptrs);
				pfree(endPtrs);
				pfree(myCheck);
				PG_RETURN_BOOL(res);
			}
		}
		pfree(ptrs);
		pfree(endPtrs);
		pfree(myCheck);
	}

	PG_RETURN_BOOL(res);
}

static float weights[] = {0.1f, 0.2f, 0.4f, 1.0f};

#define wpos(wep)	( w[ WEP_GETWEIGHT(wep) ] )
/* A dummy WordEntryPos array to use when haspos is false */
static WordEntryPosVector POSNULL = {
	1,							/* Number of elements that follow */
	{0}
};

static float calc_rank_or(float *w, char **ptrs, int size);

static float
calc_rank_and(float *w, char **ptrs, int size)
{
	int			i,
				k,
				l,
				p;
	WordEntryPos post,
			   ct;
	int32		dimt,
				lenct,
				dist;
	float		res = -1.0;
	char		*ptrt, *ptrc;

	if (size < 2)
	{
		return calc_rank_or(w, ptrs, size);
	}
	WEP_SETPOS(POSNULL.pos[0], MAXENTRYPOS - 1);

	for (i = 0; i < size; i++)
	{
		if (ptrs[i])
		{
			ptrt = readVarbyte(ptrs[i], &dimt);
		}
		else
		{
			dimt = POSNULL.npos;
			ptrt = (char *)POSNULL.pos;
		}
		for (k = 0; k < i; k++)
		{
			if (ptrs[i])
				readVarbyte(ptrs[i], &lenct);
			else
				lenct = POSNULL.npos;
			post = 0;
			for (l = 0; l < dimt; l++)
			{
				ptrt = decompress_pos(ptrt, &post);
				ct = 0;
				if (ptrs[i])
					ptrc = readVarbyte(ptrs[i], NULL);
				else
					ptrc = (char *)POSNULL.pos;
				for (p = 0; p < lenct; p++)
				{
					ptrc = decompress_pos(ptrc, &ct);
					dist = Abs((int) WEP_GETPOS(post) - (int) WEP_GETPOS(ct));
					if (dist || (dist == 0 && (ptrt == (char *)POSNULL.pos || ptrc == (char *)POSNULL.pos)))
					{
						float		curw;

						if (!dist)
							dist = MAXENTRYPOS;
						curw = sqrt(wpos(post) * wpos(ct) * word_distance(dist));
						res = (res < 0) ? curw : 1.0 - (1.0 - res) * (1.0 - curw);
					}
				}
			}
		}

	}
	return res;
}

static float
calc_rank_or(float *w, char **ptrs, int size)
{
	WordEntryPos post;
	int32		dimt,
				j,
				i;
	float		res = 0.0;
	char *ptrt;

	for (i = 0; i < size; i++)
	{
		float		resj,
					wjm;
		int32		jm;

		if (ptrs[i])
		{
			ptrt = readVarbyte(ptrs[i], &dimt);
		}
		else
		{
			dimt = POSNULL.npos;
			ptrt = (char *)POSNULL.pos;
		}

		resj = 0.0;
		wjm = -1.0;
		jm = 0;
		post = 0;
		for (j = 0; j < dimt; j++)
		{
			ptrt = decompress_pos(ptrt, &post);
			resj = resj + wpos(post) / ((j + 1) * (j + 1));
			if (wpos(post) > wjm)
			{
				wjm = wpos(post);
				jm = j;
			}
		}
/*
		limit (sum(i/i^2),i->inf) = pi^2/6
		resj = sum(wi/i^2),i=1,noccurence,
		wi - should be sorted desc,
		don't sort for now, just choose maximum weight. This should be corrected
		Oleg Bartunov
*/
		res = res + (wjm + resj - wjm / ((jm + 1) * (jm + 1))) / 1.64493406685;

	}
	if (size > 0)
		res = res / size;
	return res;
}

static float
calc_rank(float *w, TSQuery q, char **ptrs, int size)
{
	QueryItem  *item = GETQUERY(q);
	float		res = 0.0;

	if (!size || !q->size)
		return 0.0;

	/* XXX: What about NOT? */
	res = (item->type == QI_OPR && item->qoperator.oper == OP_AND) ?
		calc_rank_and(w, ptrs, size) : calc_rank_or(w, ptrs, size);

	if (res < 0)
		res = 1e-20f;

	return res;
}

Datum
gin_tsvector_array_distance(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	uint32		nentries = PG_GETARG_UINT32(3);
	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	float		max_rank = -1.0;
	float8		res;

	if (query->size > 0)
	{
		QueryItem  *item;
		GinChkVal	gcv;
		int i, first_i = -1;
		char **ptrs, **endPtrs;
		bool *myCheck;

		ptrs = (char **)palloc(sizeof(char *) * nentries);
		endPtrs = (char **)palloc(sizeof(char *) * nentries);
		myCheck = (bool *)palloc0(sizeof(bool) * nentries);
		for (i = 0; i < nentries; i++)
		{
			if (check[i] && !addInfoIsNull[i])
			{
				ptrs[i] =  VARDATA_ANY(DatumGetPointer(addInfo[i]));
				endPtrs[i] = ptrs[i] + VARSIZE_ANY_EXHDR(DatumGetPointer(addInfo[i]));
				if (first_i < 0)
					first_i = i;
			}
			else
			{
				ptrs[i] = NULL;
				endPtrs[i] = NULL;
			}
		}

		gcv.first_item = item = GETQUERY(query);
		gcv.check = myCheck;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = recheck;

		while (first_i >= 0 && ptrs[first_i] < endPtrs[first_i])
		{
			float rank;
			rank = calc_rank(weights, query, ptrs, nentries);
			max_rank = Max(max_rank, rank);
			for (i = 0; i < nentries; i++)
			{
				if (check[i] && !addInfoIsNull[i])
				{
					int npos;
					ptrs[i] = readVarbyte(ptrs[i], &npos);
					if (npos > 0)
					{
						uint16 pos;
						while (npos--)
						{
							ptrs[i] = decompress_pos(ptrs[i], &pos);
						}
						myCheck[i] = true;
					}
					else
					{
						myCheck[i] = false;
					}
				}
			}
		}
		pfree(ptrs);
		pfree(endPtrs);
		pfree(myCheck);
	}

	res = 1.0 / (float8)max_rank;

	PG_RETURN_FLOAT8(res);
}

