/*-------------------------------------------------------------------------
 *
 * vodkaarrayproc.c
 *	  support functions for VODKA's indexing of any array
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/vodka/vodkaarrayproc.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/vodka.h"
#include "access/skey.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"
#include "utils/lsyscache.h"

#define JSONB_VODKA_FLAG_VALUE		0x01

#define JSONB_VODKA_FLAG_NULL		0x00
#define JSONB_VODKA_FLAG_STRING		0x02
#define JSONB_VODKA_FLAG_NUMERIC	0x04
#define JSONB_VODKA_FLAG_BOOL		0x06
#define JSONB_VODKA_FLAG_TRUE		0x08
#define JSONB_VODKA_FLAG_NAN		0x08
#define JSONB_VODKA_FLAG_NEGATIVE	0x10

#define JSONB_VODKA_FLAG_ARRAY		0x02


Datum
vodkajsonbconfig(PG_FUNCTION_ARGS)
{
	/* VodkaConfigIn *in = (VodkaConfigIn *)PG_GETARG_POINTER(0); */
	VodkaConfigOut *out = (VodkaConfigOut *)PG_GETARG_POINTER(1);

	out->entryOpclass = BYTEA_SPGIST_OPS_OID;
	out->entryEqualOperator = ByteaEqualOperator;
	PG_RETURN_VOID();
}

typedef struct PathStack
{
	char			   *s;
	int					len;
	struct PathStack   *parent;
}	PathStack;

static int
get_ndigits(Numeric val)
{
	const NumericDigit *digits;
	int					ndigits;

	ndigits = NUMERIC_NDIGITS(val);
	digits = NUMERIC_DIGITS(val);

	while (ndigits > 0 && *digits == 0)
	{
		ndigits--;
		digits++;
	}
	return ndigits;
}

static void
write_numeric_key(Pointer ptr, Numeric val)
{
	*ptr = JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_NUMERIC;
	if (NUMERIC_IS_NAN(val))
	{
		*ptr |= JSONB_VODKA_FLAG_NAN;
	}
	else
	{
		const NumericDigit *digits = NUMERIC_DIGITS(val);
		int ndigits = NUMERIC_NDIGITS(val);
		int weight = NUMERIC_WEIGHT(val);
		int sign = NUMERIC_SIGN(val);

		if (sign == NUMERIC_NEG)
			*ptr |= JSONB_VODKA_FLAG_NEGATIVE;
		ptr++;

		while (ndigits > 0 && *digits == 0)
		{
			ndigits--;
			digits++;
		}

		memcpy(ptr, &weight, sizeof(weight));
		ptr += sizeof(weight);

		memcpy(ptr, digits, sizeof(NumericDigit) * ndigits);
		ptr += sizeof(NumericDigit) * ndigits;

		*ptr = 0;
	}
}

static bytea *
get_vodka_key(PathStack *stack, const JsonbValue *val)
{
	bytea *result;
	int totallen = VARHDRSZ, vallen;
	PathStack *tmp;
	Pointer	ptr;

	tmp = stack;
	while (tmp)
	{
		if (tmp->s)
		{
			totallen += tmp->len + 2;
		}
		else
		{
			totallen++;
		}
		tmp = tmp->parent;
	}

	switch (val->type)
	{
		case jbvNull:
		case jbvBool:
			vallen = 1;
			break;
		case jbvString:
			vallen = val->val.string.len + 2;
			break;
		case jbvNumeric:
			if (NUMERIC_IS_NAN(val->val.numeric))
				vallen = 1;
			else
				vallen = get_ndigits(val->val.numeric) + 6;
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
	}

	totallen += vallen;
	result = (bytea *)palloc(totallen);
	SET_VARSIZE(result, totallen);
	ptr = (Pointer)result + totallen - vallen;

	tmp = stack;
	while (tmp)
	{
		if (tmp->s)
		{
			ptr -= tmp->len + 2;
			ptr[0] = 0;
			memcpy(ptr + 1, tmp->s, tmp->len);
			ptr[tmp->len + 1] = 0;
		}
		else
		{
			ptr--;
			*ptr = JSONB_VODKA_FLAG_ARRAY;
		}
		tmp = tmp->parent;
	}

	ptr = (Pointer)result + totallen - vallen;

	switch (val->type)
	{
		case jbvNull:
			*ptr = JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_NULL;
			break;
		case jbvBool:
			*ptr = JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_BOOL;
			if (val->val.boolean)
				*ptr |= JSONB_VODKA_FLAG_TRUE;
			break;
		case jbvString:
			*ptr = JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_STRING;
			memcpy(ptr + 1, val->val.string.val, val->val.string.len);
			ptr[val->val.string.len + 1] = 0;
			break;
		case jbvNumeric:
			*ptr = JSONB_VODKA_FLAG_VALUE | JSONB_VODKA_FLAG_STRING;
			write_numeric_key(ptr, val->val.numeric);
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
	}

	return result;
}

/*
 * extractValue support function
 */
Datum
vodkajsonbextract(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	int			total = 2 * JB_ROOT_COUNT(jb);
	JsonbIterator *it;
	JsonbValue	v;
	PathStack *stack;
	int			i = 0,
				r;
	Datum	   *entries = NULL;

	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = JsonbIteratorInit(VARDATA(jb));
	stack = NULL;

	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		PathStack  *tmp;

		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch (r)
		{
			case WJB_BEGIN_ARRAY:
			case WJB_BEGIN_OBJECT:
				tmp = stack;
				stack = (PathStack *) palloc(sizeof(PathStack));
				stack->s = NULL;
				stack->len = 0;
				stack->parent = tmp;
				break;
			case WJB_KEY:
				/* Initialize hash from parent */
				stack->s = v.val.string.val;
				stack->len = v.val.string.len;
				break;
			case WJB_ELEM:
			case WJB_VALUE:
				entries[i++] = PointerGetDatum(get_vodka_key(stack, &v));
				break;
			case WJB_END_ARRAY:
			case WJB_END_OBJECT:
				/* Pop the stack */
				tmp = stack->parent;
				pfree(stack);
				stack = tmp;
				break;
			default:
				elog(ERROR, "invalid JsonbIteratorNext rc: %d", r);
		}
	}

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

/*
 * extractQuery support function
 */
Datum
vodkaqueryjsonbextract(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(3);
	Datum	   *entries;
	VodkaKey   *keys;
	int			i;

	if (strategy != JsonbContainsStrategyNumber)
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	/* Query is a jsonb, so just apply gin_extract_jsonb... */
	entries = (Datum *)
		DatumGetPointer(DirectFunctionCall2(vodkajsonbextract,
											PG_GETARG_DATUM(0),
											PointerGetDatum(nentries)));

	keys = (VodkaKey *)palloc(sizeof(VodkaKey) * (*nentries));

	for (i = 0; i < *nentries; i++)
	{
		keys[i].value = entries[i];
		keys[i].isnull = false;
		keys[i].extra = NULL;
		keys[i].operator = ByteaEqualOperator;
	}

	/* ...although "contains {}" requires a full index scan */
	if (entries == NULL)
		*searchMode = VODKA_SEARCH_MODE_ALL;

	PG_RETURN_POINTER(keys);
}

/*
 * consistent support function
 */
Datum
vodkajsonbconsistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* ArrayType  *query = PG_GETARG_ARRAYTYPE_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(4);
	VodkaKey   *queryKeys = (VodkaKey *) PG_GETARG_POINTER(5);
	bool		res;
	int32		i;

	switch (strategy)
	{
		case JsonbContainsStrategyNumber:
			/* result is not lossy */
			*recheck = false;
			/* must have all elements in check[] true, and no nulls */
			res = true;
			for (i = 0; i < nkeys; i++)
			{
				if (!check[i] || queryKeys[i].isnull)
				{
					res = false;
					break;
				}
			}
			break;
		default:
			elog(ERROR, "vodkajsonbconsistent: unknown strategy number: %d",
				 strategy);
			res = false;
	}

	PG_RETURN_BOOL(res);
}

/*
 * triconsistent support function
 */
Datum
vodkajsonbtriconsistent(PG_FUNCTION_ARGS)
{
	VodkaTernaryValue *check = (VodkaTernaryValue *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* ArrayType  *query = PG_GETARG_ARRAYTYPE_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	VodkaKey   *queryKeys = (VodkaKey *) PG_GETARG_POINTER(4);
	VodkaTernaryValue res;
	int32		i;

	switch (strategy)
	{
		case JsonbContainsStrategyNumber:
			/* must have all elements in check[] true, and no nulls */
			res = VODKA_TRUE;
			for (i = 0; i < nkeys; i++)
			{
				if (check[i] == VODKA_FALSE || queryKeys[i].isnull)
				{
					res = VODKA_FALSE;
					break;
				}
				if (check[i] == VODKA_MAYBE)
				{
					res = VODKA_MAYBE;
				}
			}
			break;
		default:
			elog(ERROR, "vodkajsonbconsistent: unknown strategy number: %d",
				 strategy);
			res = false;
	}

	PG_RETURN_VODKA_TERNARY_VALUE(res);
}
