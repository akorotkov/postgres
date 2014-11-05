/*-------------------------------------------------------------------------
 *
 * tsginidx.c
 *	 GIN support functions for tsvector_ops
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tsginidx.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin.h"
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"

Datum
gin_cmp_tslexeme(PG_FUNCTION_ARGS)
{
	text	   *a = PG_GETARG_TEXT_PP(0);
	text	   *b = PG_GETARG_TEXT_PP(1);
	int			cmp;

	cmp = tsCompareString(VARDATA_ANY(a), VARSIZE_ANY_EXHDR(a),
						  VARDATA_ANY(b), VARSIZE_ANY_EXHDR(b),
						  false);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_INT32(cmp);
}

Datum
gin_cmp_prefix(PG_FUNCTION_ARGS)
{
	text	   *a = PG_GETARG_TEXT_PP(0);
	text	   *b = PG_GETARG_TEXT_PP(1);

#ifdef NOT_USED
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	Pointer		extra_data = PG_GETARG_POINTER(3);
#endif
	int			cmp;

	cmp = tsCompareString(VARDATA_ANY(a), VARSIZE_ANY_EXHDR(a),
						  VARDATA_ANY(b), VARSIZE_ANY_EXHDR(b),
						  true);

	if (cmp < 0)
		cmp = 1;				/* prevent continue scan */

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_INT32(cmp);
}

#define SIXTHBIT 0x20
#define LOWERMASK 0x1F

static int
compress_pos(char *target, uint16 *pos, int npos)
{
	int i;
	uint16 prev = 0, delta;
	char *ptr;

	ptr = target;
	for (i = 0; i < npos; i++)
	{
		delta = WEP_GETPOS(pos[i]) - WEP_GETPOS(prev);

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
				*ptr = delta | (WEP_GETWEIGHT(pos[i]) << 5);
				ptr++;
				break;
			}
		}
		prev = pos[i];
	}
	return ptr - target;
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


Datum
gin_extract_tsvector(PG_FUNCTION_ARGS)
{
	TSVector	vector = PG_GETARG_TSVECTOR(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   **addInfo = (Datum **) PG_GETARG_POINTER(3);
	bool	   **addInfoIsNull = (bool **) PG_GETARG_POINTER(4);
	Datum	   *entries = NULL;

	*nentries = vector->size;
	if (vector->size > 0)
	{
		int			i;
		WordEntry  *we = ARRPTR(vector);
		WordEntryPosVector *posVec;

		entries = (Datum *) palloc(sizeof(Datum) * vector->size);
		*addInfo = (Datum *) palloc(sizeof(Datum) * vector->size);
		*addInfoIsNull = (bool *) palloc(sizeof(bool) * vector->size);

		for (i = 0; i < vector->size; i++)
		{
			text	   *txt;
			bytea	   *posData;
			int			posDataSize;

			txt = cstring_to_text_with_len(STRPTR(vector) + we->pos, we->len);
			entries[i] = PointerGetDatum(txt);

			if (we->haspos)
			{
				posVec = _POSVECPTR(vector, we);
				posDataSize = VARHDRSZ + 2 * posVec->npos * sizeof(WordEntryPos);
				posData = (bytea *)palloc(posDataSize);
				posDataSize = compress_pos(posData->vl_dat, posVec->pos, posVec->npos) + VARHDRSZ;
				SET_VARSIZE(posData, posDataSize);

				(*addInfo)[i] = PointerGetDatum(posData);
				(*addInfoIsNull)[i] = false;
			}
			else
			{
				(*addInfo)[i] = (Datum)0;
				(*addInfoIsNull)[i] = true;
			}
			we++;
		}
	}

	PG_FREE_IF_COPY(vector, 0);
	PG_RETURN_POINTER(entries);
}

Datum
gin_extract_tsquery(PG_FUNCTION_ARGS)
{
	TSQuery		query = PG_GETARG_TSQUERY(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);

	/* StrategyNumber strategy = PG_GETARG_UINT16(2); */
	bool	  **ptr_partialmatch = (bool **) PG_GETARG_POINTER(3);
	Pointer   **extra_data = (Pointer **) PG_GETARG_POINTER(4);

	/* bool   **nullFlags = (bool **) PG_GETARG_POINTER(5); */
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries = NULL;

	*nentries = 0;

	if (query->size > 0)
	{
		QueryItem  *item = GETQUERY(query);
		int32		i,
					j;
		bool	   *partialmatch;
		int		   *map_item_operand;
		char       *operand = GETOPERAND(query);
		QueryOperand **operands;

		/*
		 * If the query doesn't have any required positive matches (for
		 * instance, it's something like '! foo'), we have to do a full index
		 * scan.
		 */
		if (tsquery_requires_match(item))
			*searchMode = GIN_SEARCH_MODE_DEFAULT;
		else
			*searchMode = GIN_SEARCH_MODE_ALL;

		*nentries = query->size;
		operands = SortAndUniqItems(query, nentries);

		entries = (Datum *) palloc(sizeof(Datum) * (*nentries));
		partialmatch = *ptr_partialmatch = (bool *) palloc(sizeof(bool) * (*nentries));

		/*
		 * Make map to convert item's number to corresponding operand's (the
		 * same, entry's) number. Entry's number is used in check array in
		 * consistent method. We use the same map for each entry.
		 */
		*extra_data = (Pointer *) palloc(sizeof(Pointer) * (*nentries));
		map_item_operand = (int *) palloc0(sizeof(int) * query->size);

		for (i = 0; i < (*nentries); i++)
		{
			text	   *txt;

			txt = cstring_to_text_with_len(GETOPERAND(query) + operands[i]->distance,
											operands[i]->length);
			entries[i] = PointerGetDatum(txt);
			partialmatch[i] = operands[i]->prefix;
			(*extra_data)[i] = (Pointer) map_item_operand;
		}

		/* Now rescan the VAL items and fill in the arrays */
		for (j = 0; j < query->size; j++)
		{
			if (item[j].type == QI_VAL)
			{
				QueryOperand *val = &item[j].qoperand;
				bool found = false;

				for (i = 0; i < (*nentries); i++)
				{
					if (!tsCompareString(operand + operands[i]->distance, operands[i]->length,
							operand + val->distance, val->length,
							false))
					{
						map_item_operand[j] = i;
						found = true;
						break;
					}
				}

				if (!found)
					elog(ERROR, "Operand not found!");
			}
		}
	}

	PG_FREE_IF_COPY(query, 0);

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
gin_tsquery_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = FALSE;

	/* The query requires recheck only if it involves weights */
	*recheck = false;

	if (query->size > 0)
	{
		QueryItem  *item;
		GinChkVal	gcv;

		/*
		 * check-parameter array has one entry for each value (operand) in the
		 * query.
		 */
		gcv.first_item = item = GETQUERY(query);
		gcv.check = check;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = recheck;

		res = TS_execute(GETQUERY(query),
						 &gcv,
						 true,
						 checkcondition_gin);
	}

	PG_RETURN_BOOL(res);
}

Datum
gin_tsquery_pre_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4);
	bool	    recheck;
	bool		res = FALSE;

	if (query->size > 0)
	{
		QueryItem  *item;
		GinChkVal	gcv;

		/*
		 * check-parameter array has one entry for each value (operand) in the
		 * query.
		 */
		gcv.first_item = item = GETQUERY(query);
		gcv.check = check;
		gcv.map_item_operand = (int *) (extra_data[0]);
		gcv.need_recheck = &recheck;

		res = TS_execute(GETQUERY(query),
						 &gcv,
						 false,
						 checkcondition_gin);
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

static float calc_rank_or(float *w, Datum *addInfo, bool *addInfoIsNull, int size);

static float
calc_rank_and(float *w, Datum *addInfo, bool *addInfoIsNull, int size)
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
		return calc_rank_or(w, addInfo, addInfoIsNull, size);
	}
	WEP_SETPOS(POSNULL.pos[0], MAXENTRYPOS - 1);

	for (i = 0; i < size; i++)
	{
		if (!addInfoIsNull[i])
		{
			dimt = count_pos(VARDATA_ANY(addInfo[i]), VARSIZE_ANY_EXHDR(addInfo[i]));
			ptrt = (char *)VARDATA_ANY(addInfo[i]);
		}
		else
		{
			dimt = POSNULL.npos;
			ptrt = (char *)POSNULL.pos;
		}
		for (k = 0; k < i; k++)
		{
			if (!addInfoIsNull[k])
				lenct = count_pos(VARDATA_ANY(addInfo[k]), VARSIZE_ANY_EXHDR(addInfo[k]));
			else
				lenct = POSNULL.npos;
			post = 0;
			for (l = 0; l < dimt; l++)
			{
				ptrt = decompress_pos(ptrt, &post);
				ct = 0;
				if (!addInfoIsNull[k])
					ptrc = (char *)VARDATA_ANY(addInfo[k]);
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
calc_rank_or(float *w, Datum *addInfo, bool *addInfoIsNull, int size)
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

		if (!addInfoIsNull[i])
		{
			dimt = count_pos(VARDATA_ANY(addInfo[i]), VARSIZE_ANY_EXHDR(addInfo[i]));
			ptrt = (char *)VARDATA_ANY(addInfo[i]);
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
calc_rank(float *w, TSQuery q, Datum *addInfo, bool *addInfoIsNull, int size)
{
	QueryItem  *item = GETQUERY(q);
	float		res = 0.0;

	if (!size || !q->size)
		return 0.0;

	/* XXX: What about NOT? */
	res = (item->type == QI_OPR && item->qoperator.oper == OP_AND) ?
		calc_rank_and(w, addInfo, addInfoIsNull, size) : calc_rank_or(w, addInfo, addInfoIsNull, size);

	if (res < 0)
		res = 1e-20f;

	return res;
}

Datum
gin_tsvector_config(PG_FUNCTION_ARGS)
{
	GinConfig *config = (GinConfig *)PG_GETARG_POINTER(0);
	config->addInfoTypeOid = BYTEAOID;
	PG_RETURN_VOID();
}

Datum
gin_tsquery_distance(PG_FUNCTION_ARGS)
{
	/* bool	   *check = (bool *) PG_GETARG_POINTER(0); */

	/* StrategyNumber strategy = PG_GETARG_UINT16(1); */
	TSQuery		query = PG_GETARG_TSQUERY(2);

	int32	nkeys = PG_GETARG_INT32(3);
	/* Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	Datum	   *addInfo = (Datum *) PG_GETARG_POINTER(8);
	bool	   *addInfoIsNull = (bool *) PG_GETARG_POINTER(9);
	float8 res;

	res = 1.0 / (float8)calc_rank(weights, query, addInfo, addInfoIsNull, nkeys);

	PG_RETURN_FLOAT8(res);
}

/*
 * Formerly, gin_extract_tsvector had only two arguments.  Now it has three,
 * but we still need a pg_proc entry with two args to support reloading
 * pre-9.1 contrib/tsearch2 opclass declarations.  This compatibility
 * function should go away eventually.  (Note: you might say "hey, but the
 * code above is only *using* two args, so let's just declare it that way".
 * If you try that you'll find the opr_sanity regression test complains.)
 */
Datum
gin_extract_tsvector_2args(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() < 3)			/* should not happen */
		elog(ERROR, "gin_extract_tsvector requires three arguments");
	return gin_extract_tsvector(fcinfo);
}

/*
 * Likewise, we need a stub version of gin_extract_tsquery declared with
 * only five arguments.
 */
Datum
gin_extract_tsquery_5args(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() < 7)			/* should not happen */
		elog(ERROR, "gin_extract_tsquery requires seven arguments");
	return gin_extract_tsquery(fcinfo);
}

/*
 * Likewise, we need a stub version of gin_tsquery_consistent declared with
 * only six arguments.
 */
Datum
gin_tsquery_consistent_6args(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() < 8)			/* should not happen */
		elog(ERROR, "gin_tsquery_consistent requires eight arguments");
	return gin_tsquery_consistent(fcinfo);
}
