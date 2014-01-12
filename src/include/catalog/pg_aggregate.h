/*-------------------------------------------------------------------------
 *
 * pg_aggregate.h
 *	  definition of the system "aggregate" relation (pg_aggregate)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_aggregate.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AGGREGATE_H
#define PG_AGGREGATE_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------------------------------------------------------
 *		pg_aggregate definition.
 *
 *		cpp turns this into typedef struct FormData_pg_aggregate
 *
 *	aggfnoid			pg_proc OID of the aggregate itself
 *	aggkind				aggregate kind, see AGGKIND_ categories below
 *	aggnumdirectargs	number of arguments that are "direct" arguments
 *	aggtransfn			transition function
 *	aggfinalfn			final function (0 if none)
 *	aggsortop			associated sort operator (0 if none)
 *	aggtranstype		type of aggregate's transition (state) data
 *	aggtransspace		estimated size of state data (0 for default estimate)
 *	agginitval			initial value for transition state (can be NULL)
 * ----------------------------------------------------------------
 */
#define AggregateRelationId  2600

CATALOG(pg_aggregate,2600) BKI_WITHOUT_OIDS
{
	regproc		aggfnoid;
	char		aggkind;
	int16		aggnumdirectargs;
	regproc		aggtransfn;
	regproc		aggfinalfn;
	Oid			aggsortop;
	Oid			aggtranstype;
	int32		aggtransspace;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		agginitval;
#endif
} FormData_pg_aggregate;

/* ----------------
 *		Form_pg_aggregate corresponds to a pointer to a tuple with
 *		the format of pg_aggregate relation.
 * ----------------
 */
typedef FormData_pg_aggregate *Form_pg_aggregate;

/* ----------------
 *		compiler constants for pg_aggregate
 * ----------------
 */

#define Natts_pg_aggregate					9
#define Anum_pg_aggregate_aggfnoid			1
#define Anum_pg_aggregate_aggkind			2
#define Anum_pg_aggregate_aggnumdirectargs	3
#define Anum_pg_aggregate_aggtransfn		4
#define Anum_pg_aggregate_aggfinalfn		5
#define Anum_pg_aggregate_aggsortop			6
#define Anum_pg_aggregate_aggtranstype		7
#define Anum_pg_aggregate_aggtransspace		8
#define Anum_pg_aggregate_agginitval		9

/*
 * Symbolic values for aggkind column.	We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments) and from hypothetical-set aggregates
 * (which are a subclass of ordered-set aggregates in which the last
 * direct arguments have to match up in number and datatypes with the
 * aggregated arguments).
 */
#define AGGKIND_NORMAL			'n'
#define AGGKIND_ORDERED_SET		'o'
#define AGGKIND_HYPOTHETICAL	'h'

/* Use this macro to test for "ordered-set agg including hypothetical case" */
#define AGGKIND_IS_ORDERED_SET(kind)  ((kind) != AGGKIND_NORMAL)


/* ----------------
 * initial contents of pg_aggregate
 * ---------------
 */

/* avg */
DATA(insert ( 2100	n 0 int8_avg_accum	numeric_avg		0	2281	128 _null_ ));
DATA(insert ( 2101	n 0 int4_avg_accum	int8_avg		0	1016	0	"{0,0}" ));
DATA(insert ( 2102	n 0 int2_avg_accum	int8_avg		0	1016	0	"{0,0}" ));
DATA(insert ( 2103	n 0 numeric_avg_accum	numeric_avg 0	2281	128 _null_ ));
DATA(insert ( 2104	n 0 float4_accum	float8_avg		0	1022	0	"{0,0,0}" ));
DATA(insert ( 2105	n 0 float8_accum	float8_avg		0	1022	0	"{0,0,0}" ));
DATA(insert ( 2106	n 0 interval_accum	interval_avg	0	1187	0	"{0 second,0 second}" ));

/* sum */
DATA(insert ( 2107	n 0 int8_avg_accum	numeric_sum		0	2281	128 _null_ ));
DATA(insert ( 2108	n 0 int4_sum		-				0	20		0	_null_ ));
DATA(insert ( 2109	n 0 int2_sum		-				0	20		0	_null_ ));
DATA(insert ( 2110	n 0 float4pl		-				0	700		0	_null_ ));
DATA(insert ( 2111	n 0 float8pl		-				0	701		0	_null_ ));
DATA(insert ( 2112	n 0 cash_pl			-				0	790		0	_null_ ));
DATA(insert ( 2113	n 0 interval_pl		-				0	1186	0	_null_ ));
DATA(insert ( 2114	n 0 numeric_avg_accum	numeric_sum 0	2281	128 _null_ ));

/* max */
DATA(insert ( 2115	n 0 int8larger		-				413		20		0	_null_ ));
DATA(insert ( 2116	n 0 int4larger		-				521		23		0	_null_ ));
DATA(insert ( 2117	n 0 int2larger		-				520		21		0	_null_ ));
DATA(insert ( 2118	n 0 oidlarger		-				610		26		0	_null_ ));
DATA(insert ( 2119	n 0 float4larger	-				623		700		0	_null_ ));
DATA(insert ( 2120	n 0 float8larger	-				674		701		0	_null_ ));
DATA(insert ( 2121	n 0 int4larger		-				563		702		0	_null_ ));
DATA(insert ( 2122	n 0 date_larger		-				1097	1082	0	_null_ ));
DATA(insert ( 2123	n 0 time_larger		-				1112	1083	0	_null_ ));
DATA(insert ( 2124	n 0 timetz_larger	-				1554	1266	0	_null_ ));
DATA(insert ( 2125	n 0 cashlarger		-				903		790		0	_null_ ));
DATA(insert ( 2126	n 0 timestamp_larger	-			2064	1114	0	_null_ ));
DATA(insert ( 2127	n 0 timestamptz_larger	-			1324	1184	0	_null_ ));
DATA(insert ( 2128	n 0 interval_larger -				1334	1186	0	_null_ ));
DATA(insert ( 2129	n 0 text_larger		-				666		25		0	_null_ ));
DATA(insert ( 2130	n 0 numeric_larger	-				1756	1700	0	_null_ ));
DATA(insert ( 2050	n 0 array_larger	-				1073	2277	0	_null_ ));
DATA(insert ( 2244	n 0 bpchar_larger	-				1060	1042	0	_null_ ));
DATA(insert ( 2797	n 0 tidlarger		-				2800	27		0	_null_ ));
DATA(insert ( 3526	n 0 enum_larger		-				3519	3500	0	_null_ ));

/* min */
DATA(insert ( 2131	n 0 int8smaller		-				412		20		0	_null_ ));
DATA(insert ( 2132	n 0 int4smaller		-				97		23		0	_null_ ));
DATA(insert ( 2133	n 0 int2smaller		-				95		21		0	_null_ ));
DATA(insert ( 2134	n 0 oidsmaller		-				609		26		0	_null_ ));
DATA(insert ( 2135	n 0 float4smaller	-				622		700		0	_null_ ));
DATA(insert ( 2136	n 0 float8smaller	-				672		701		0	_null_ ));
DATA(insert ( 2137	n 0 int4smaller		-				562		702		0	_null_ ));
DATA(insert ( 2138	n 0 date_smaller	-				1095	1082	0	_null_ ));
DATA(insert ( 2139	n 0 time_smaller	-				1110	1083	0	_null_ ));
DATA(insert ( 2140	n 0 timetz_smaller	-				1552	1266	0	_null_ ));
DATA(insert ( 2141	n 0 cashsmaller		-				902		790		0	_null_ ));
DATA(insert ( 2142	n 0 timestamp_smaller	-			2062	1114	0	_null_ ));
DATA(insert ( 2143	n 0 timestamptz_smaller -			1322	1184	0	_null_ ));
DATA(insert ( 2144	n 0 interval_smaller	-			1332	1186	0	_null_ ));
DATA(insert ( 2145	n 0 text_smaller	-				664		25		0	_null_ ));
DATA(insert ( 2146	n 0 numeric_smaller -				1754	1700	0	_null_ ));
DATA(insert ( 2051	n 0 array_smaller	-				1072	2277	0	_null_ ));
DATA(insert ( 2245	n 0 bpchar_smaller	-				1058	1042	0	_null_ ));
DATA(insert ( 2798	n 0 tidsmaller		-				2799	27		0	_null_ ));
DATA(insert ( 3527	n 0 enum_smaller	-				3518	3500	0	_null_ ));

/* count */
DATA(insert ( 2147	n 0 int8inc_any		-				0		20		0	"0" ));
DATA(insert ( 2803	n 0 int8inc			-				0		20		0	"0" ));

/* var_pop */
DATA(insert ( 2718	n 0 int8_accum	numeric_var_pop 0	2281	128 _null_ ));
DATA(insert ( 2719	n 0 int4_accum	numeric_var_pop 0	2281	128 _null_ ));
DATA(insert ( 2720	n 0 int2_accum	numeric_var_pop 0	2281	128 _null_ ));
DATA(insert ( 2721	n 0 float4_accum	float8_var_pop 0	1022	0	"{0,0,0}" ));
DATA(insert ( 2722	n 0 float8_accum	float8_var_pop 0	1022	0	"{0,0,0}" ));
DATA(insert ( 2723	n 0 numeric_accum	numeric_var_pop 0	2281	128 _null_ ));

/* var_samp */
DATA(insert ( 2641	n 0 int8_accum	numeric_var_samp	0	2281	128 _null_ ));
DATA(insert ( 2642	n 0 int4_accum	numeric_var_samp	0	2281	128 _null_ ));
DATA(insert ( 2643	n 0 int2_accum	numeric_var_samp	0	2281	128 _null_ ));
DATA(insert ( 2644	n 0 float4_accum	float8_var_samp 0	1022	0	"{0,0,0}" ));
DATA(insert ( 2645	n 0 float8_accum	float8_var_samp 0	1022	0	"{0,0,0}" ));
DATA(insert ( 2646	n 0 numeric_accum	numeric_var_samp 0	2281	128 _null_ ));

/* variance: historical Postgres syntax for var_samp */
DATA(insert ( 2148	n 0 int8_accum	numeric_var_samp	0	2281	128 _null_ ));
DATA(insert ( 2149	n 0 int4_accum	numeric_var_samp	0	2281	128 _null_ ));
DATA(insert ( 2150	n 0 int2_accum	numeric_var_samp	0	2281	128 _null_ ));
DATA(insert ( 2151	n 0 float4_accum	float8_var_samp 0	1022	0	"{0,0,0}" ));
DATA(insert ( 2152	n 0 float8_accum	float8_var_samp 0	1022	0	"{0,0,0}" ));
DATA(insert ( 2153	n 0 numeric_accum	numeric_var_samp 0	2281	128 _null_ ));

/* stddev_pop */
DATA(insert ( 2724	n 0 int8_accum	numeric_stddev_pop		0	2281	128 _null_ ));
DATA(insert ( 2725	n 0 int4_accum	numeric_stddev_pop		0	2281	128 _null_ ));
DATA(insert ( 2726	n 0 int2_accum	numeric_stddev_pop		0	2281	128 _null_ ));
DATA(insert ( 2727	n 0 float4_accum	float8_stddev_pop	0	1022	0	"{0,0,0}" ));
DATA(insert ( 2728	n 0 float8_accum	float8_stddev_pop	0	1022	0	"{0,0,0}" ));
DATA(insert ( 2729	n 0 numeric_accum	numeric_stddev_pop	0	2281	128 _null_ ));

/* stddev_samp */
DATA(insert ( 2712	n 0 int8_accum	numeric_stddev_samp		0	2281	128 _null_ ));
DATA(insert ( 2713	n 0 int4_accum	numeric_stddev_samp		0	2281	128 _null_ ));
DATA(insert ( 2714	n 0 int2_accum	numeric_stddev_samp		0	2281	128 _null_ ));
DATA(insert ( 2715	n 0 float4_accum	float8_stddev_samp	0	1022	0	"{0,0,0}" ));
DATA(insert ( 2716	n 0 float8_accum	float8_stddev_samp	0	1022	0	"{0,0,0}" ));
DATA(insert ( 2717	n 0 numeric_accum	numeric_stddev_samp 0	2281	128 _null_ ));

/* stddev: historical Postgres syntax for stddev_samp */
DATA(insert ( 2154	n 0 int8_accum	numeric_stddev_samp		0	2281	128 _null_ ));
DATA(insert ( 2155	n 0 int4_accum	numeric_stddev_samp		0	2281	128 _null_ ));
DATA(insert ( 2156	n 0 int2_accum	numeric_stddev_samp		0	2281	128 _null_ ));
DATA(insert ( 2157	n 0 float4_accum	float8_stddev_samp	0	1022	0	"{0,0,0}" ));
DATA(insert ( 2158	n 0 float8_accum	float8_stddev_samp	0	1022	0	"{0,0,0}" ));
DATA(insert ( 2159	n 0 numeric_accum	numeric_stddev_samp 0	2281	128 _null_ ));

/* SQL2003 binary regression aggregates */
DATA(insert ( 2818	n 0 int8inc_float8_float8		-				0	20		0	"0" ));
DATA(insert ( 2819	n 0 float8_regr_accum	float8_regr_sxx			0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2820	n 0 float8_regr_accum	float8_regr_syy			0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2821	n 0 float8_regr_accum	float8_regr_sxy			0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2822	n 0 float8_regr_accum	float8_regr_avgx		0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2823	n 0 float8_regr_accum	float8_regr_avgy		0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2824	n 0 float8_regr_accum	float8_regr_r2			0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2825	n 0 float8_regr_accum	float8_regr_slope		0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2826	n 0 float8_regr_accum	float8_regr_intercept	0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2827	n 0 float8_regr_accum	float8_covar_pop		0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2828	n 0 float8_regr_accum	float8_covar_samp		0	1022	0	"{0,0,0,0,0,0}" ));
DATA(insert ( 2829	n 0 float8_regr_accum	float8_corr				0	1022	0	"{0,0,0,0,0,0}" ));

/* boolean-and and boolean-or */
DATA(insert ( 2517	n 0 booland_statefunc	-			58	16		0	_null_ ));
DATA(insert ( 2518	n 0 boolor_statefunc	-			59	16		0	_null_ ));
DATA(insert ( 2519	n 0 booland_statefunc	-			58	16		0	_null_ ));

/* bitwise integer */
DATA(insert ( 2236	n 0 int2and		-					0	21		0	_null_ ));
DATA(insert ( 2237	n 0 int2or		-					0	21		0	_null_ ));
DATA(insert ( 2238	n 0 int4and		-					0	23		0	_null_ ));
DATA(insert ( 2239	n 0 int4or		-					0	23		0	_null_ ));
DATA(insert ( 2240	n 0 int8and		-					0	20		0	_null_ ));
DATA(insert ( 2241	n 0 int8or		-					0	20		0	_null_ ));
DATA(insert ( 2242	n 0 bitand		-					0	1560	0	_null_ ));
DATA(insert ( 2243	n 0 bitor		-					0	1560	0	_null_ ));

/* xml */
DATA(insert ( 2901	n 0 xmlconcat2	-					0	142		0	_null_ ));

/* array */
DATA(insert ( 2335	n 0 array_agg_transfn	array_agg_finalfn	0	2281	0	_null_ ));

/* text */
DATA(insert ( 3538	n 0 string_agg_transfn	string_agg_finalfn	0	2281	0	_null_ ));

/* bytea */
DATA(insert ( 3545	n 0 bytea_string_agg_transfn	bytea_string_agg_finalfn	0	2281	0	_null_ ));

/* json */
DATA(insert ( 3175	n 0 json_agg_transfn	json_agg_finalfn	0	2281	0	_null_ ));

/* ordered-set and hypothetical-set aggregates */
DATA(insert ( 3972	o 1 ordered_set_transition			percentile_disc_final					0	2281	0	_null_ ));
DATA(insert ( 3974	o 1 ordered_set_transition			percentile_cont_float8_final			0	2281	0	_null_ ));
DATA(insert ( 3976	o 1 ordered_set_transition			percentile_cont_interval_final			0	2281	0	_null_ ));
DATA(insert ( 3978	o 1 ordered_set_transition			percentile_disc_multi_final				0	2281	0	_null_ ));
DATA(insert ( 3980	o 1 ordered_set_transition			percentile_cont_float8_multi_final		0	2281	0	_null_ ));
DATA(insert ( 3982	o 1 ordered_set_transition			percentile_cont_interval_multi_final	0	2281	0	_null_ ));
DATA(insert ( 3984	o 0 ordered_set_transition			mode_final								0	2281	0	_null_ ));
DATA(insert ( 3986	h 1 ordered_set_transition_multi	rank_final								0	2281	0	_null_ ));
DATA(insert ( 3988	h 1 ordered_set_transition_multi	percent_rank_final						0	2281	0	_null_ ));
DATA(insert ( 3990	h 1 ordered_set_transition_multi	cume_dist_final							0	2281	0	_null_ ));
DATA(insert ( 3992	h 1 ordered_set_transition_multi	dense_rank_final						0	2281	0	_null_ ));


/*
 * prototypes for functions in pg_aggregate.c
 */
extern Oid AggregateCreate(const char *aggName,
				Oid aggNamespace,
				char aggKind,
				int numArgs,
				int numDirectArgs,
				oidvector *parameterTypes,
				Datum allParameterTypes,
				Datum parameterModes,
				Datum parameterNames,
				List *parameterDefaults,
				Oid variadicArgType,
				List *aggtransfnName,
				List *aggfinalfnName,
				List *aggsortopName,
				Oid aggTransType,
				int32 aggTransSpace,
				const char *agginitval);

#endif   /* PG_AGGREGATE_H */
