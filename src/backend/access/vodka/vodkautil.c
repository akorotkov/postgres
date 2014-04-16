/*-------------------------------------------------------------------------
 *
 * vodkautil.c
 *	  utilities routines for the postgres inverted index access method.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/vodka/vodkautil.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/vodka_private.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static void
initEntryIndex(VodkaState *state, VodkaConfigOut *configOut)
{
	Relation 			index = state->index;
	Relation			entryIndex = &state->entryTree;
	Form_pg_class		relationForm;
	Form_pg_attribute	attr;
	HeapTuple			tuple;
	Form_pg_am			aform;
	Form_pg_opclass		oform;
	Form_pg_type		tform;
	int					nsupport;
	OpClassCacheEnt	   *opcentry;
	Oid					amid;
	Oid					typeid;

	tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(configOut->entryOpclass));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator class %u",
				configOut->entryOpclass);
	oform = (Form_pg_opclass)GETSTRUCT(tuple);
	amid = oform->opcmethod;
	typeid = oform->opcintype;
	state->entryTreeOpFamily = oform->opcfamily;
	ReleaseSysCache(tuple);

	memset(&state->entryTree, 0, sizeof(RelationData));
	entryIndex->rd_node = state->entryTreeNode;
	entryIndex->rd_backend = state->index->rd_backend;
	RelationOpenSmgr(entryIndex);
	relationForm = (Form_pg_class)MemoryContextAlloc(index->rd_indexcxt, CLASS_TUPLE_SIZE);
	relationForm->relpersistence = index->rd_rel->relpersistence;
	entryIndex->rd_rel = relationForm;
	entryIndex->rd_islocaltemp = index->rd_islocaltemp;
	entryIndex->rd_createSubid = index->rd_createSubid;
	entryIndex->rd_rel->relam = amid;

	/* FIXME: use different lock */
	entryIndex->rd_lockInfo = index->rd_lockInfo;

	entryIndex->rd_indexcxt = index->rd_indexcxt;
	entryIndex->rd_att = CreateTemplateTupleDesc(1, false);
	attr = entryIndex->rd_att->attrs[0];
	memset(attr, 0, sizeof(FormData_pg_attribute));

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", typeid);
	tform = (Form_pg_type)GETSTRUCT(tuple);
	attr->atttypid = typeid;
	attr->attlen = tform->typlen;
	attr->attnum = 1;
	attr->attbyval = tform->typbyval;
	attr->attstorage = tform->typstorage;
	attr->attalign = tform->typalign;
	ReleaseSysCache(tuple);

	entryIndex->rd_indcollation = (Oid *)MemoryContextAlloc(index->rd_indexcxt, sizeof(Oid));
	entryIndex->rd_indcollation[0] = DEFAULT_COLLATION_OID;

	/*
	 * Make a copy of the pg_am entry for the index's access method
	 */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(entryIndex->rd_rel->relam));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u",
				entryIndex->rd_rel->relam);
	aform = (Form_pg_am)MemoryContextAlloc(index->rd_indexcxt, sizeof(*aform));
	memcpy(aform, GETSTRUCT(tuple), sizeof *aform);
	ReleaseSysCache(tuple);
	entryIndex->rd_am = aform;

	nsupport = entryIndex->rd_am->amsupport;

	entryIndex->rd_support = (RegProcedure *)
		MemoryContextAllocZero(index->rd_indexcxt, nsupport * sizeof(RegProcedure));
	entryIndex->rd_supportinfo = (FmgrInfo *)
		MemoryContextAllocZero(index->rd_indexcxt, nsupport * sizeof(FmgrInfo));

	/* look up the info for this opclass, using a cache */
	opcentry = LookupOpclassInfo(configOut->entryOpclass,
			nsupport);
	state->entryTreeOpFamily = opcentry->opcfamily;
	memcpy(entryIndex->rd_support,
		   opcentry->supportProcs,
		   nsupport * sizeof(RegProcedure));
	state->entryEqualOperator = configOut->entryEqualOperator;
}

IndexScanDesc
prepareEntryIndexScan(VodkaState *state, Oid operator, Datum value)
{
	IndexScanDesc scanDesc;
	ScanKey key =  (ScanKey)palloc0(sizeof(ScanKeyData));
	HeapTuple	tp;
	Form_pg_amop amop_tup;
	Form_pg_operator operator_tup;

	scanDesc = (IndexScanDesc)DatumGetPointer(OidFunctionCall3(state->entryTree.rd_am->ambeginscan,
					 PointerGetDatum(&state->entryTree),
					 Int32GetDatum(1),
					 Int32GetDatum(0)));

	key->sk_argument = value;
	key->sk_attno = 1;
	key->sk_collation = DEFAULT_COLLATION_OID;

	tp = SearchSysCache1(OPEROID,
						 ObjectIdGetDatum(operator));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "%u operator doesn't exist",
				operator);
	operator_tup = (Form_pg_operator) GETSTRUCT(tp);
	key->sk_subtype = operator_tup->oprright;
	fmgr_info(operator_tup->oprcode, &key->sk_func);
	ReleaseSysCache(tp);

	tp = SearchSysCache3(AMOPOPID,
						 ObjectIdGetDatum(operator),
						 CharGetDatum(AMOP_SEARCH),
						 ObjectIdGetDatum(state->entryTreeOpFamily));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "%u operator is not in %u opfamily",
				operator, state->entryTreeOpFamily);
	amop_tup = (Form_pg_amop) GETSTRUCT(tp);
	key->sk_strategy = amop_tup->amopstrategy;
	ReleaseSysCache(tp);

	OidFunctionCall5(state->entryTree.rd_am->amrescan,
					 PointerGetDatum(scanDesc),
					 PointerGetDatum(key),
					 Int32GetDatum(1),
					 PointerGetDatum(NULL),
					 Int32GetDatum(0));

	return scanDesc;
}

/*
 * initVodkaState: fill in an empty VodkaState struct to describe the index
 *
 * Note: assorted subsidiary data is allocated in the CurrentMemoryContext.
 */
VodkaState *
initVodkaState(Relation index)
{
	TupleDesc	origTupdesc = RelationGetDescr(index);
	int			i;
	VodkaConfigIn	configIn;
	VodkaConfigOut	configOut;
	VodkaStatsData	stats;
	VodkaState	   *state;

	if (index->rd_amcache != NULL)
	{
		state = (VodkaState *)index->rd_amcache;
		return state;
	}
	state = MemoryContextAllocZero(index->rd_indexcxt, sizeof(VodkaState));

	vodkaGetStats(index, &stats);

	state->postingListLUP = stats.postingListLUP;
	state->entryTreeNode = stats.entryTreeNode;
	state->index = index;
	state->oneCol = (origTupdesc->natts == 1) ? true : false;
	state->origTupdesc = origTupdesc;

	for (i = 0; i < origTupdesc->natts; i++)
	{
		if (state->oneCol)
			state->tupdesc[i] = state->origTupdesc;
		else
		{
			state->tupdesc[i] = CreateTemplateTupleDesc(2, false);

			TupleDescInitEntry(state->tupdesc[i], (AttrNumber) 1, NULL,
							   INT2OID, -1, 0);
			TupleDescInitEntry(state->tupdesc[i], (AttrNumber) 2, NULL,
							   origTupdesc->attrs[i]->atttypid,
							   origTupdesc->attrs[i]->atttypmod,
							   origTupdesc->attrs[i]->attndims);
			TupleDescInitEntryCollation(state->tupdesc[i], (AttrNumber) 2,
										origTupdesc->attrs[i]->attcollation);
		}

		fmgr_info_copy(&(state->configFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_CONFIG_PROC),
					   CurrentMemoryContext);
		fmgr_info_copy(&(state->compareFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_COMPARE_PROC),
					   CurrentMemoryContext);
		fmgr_info_copy(&(state->extractValueFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_EXTRACTVALUE_PROC),
					   CurrentMemoryContext);
		fmgr_info_copy(&(state->extractQueryFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_EXTRACTQUERY_PROC),
					   CurrentMemoryContext);
		/*
		 * Check opclass capability to do tri-state or binary logic consistent
		 * check.
		 */
		if (index_getprocid(index, i + 1, VODKA_TRICONSISTENT_PROC) != InvalidOid)
		{
			fmgr_info_copy(&(state->triConsistentFn[i]),
			   index_getprocinfo(index, i + 1, VODKA_TRICONSISTENT_PROC),
						   CurrentMemoryContext);
		}

		if (index_getprocid(index, i + 1, VODKA_CONSISTENT_PROC) != InvalidOid)
		{
			fmgr_info_copy(&(state->consistentFn[i]),
						   index_getprocinfo(index, i + 1, VODKA_CONSISTENT_PROC),
						   CurrentMemoryContext);
		}

		if (state->consistentFn[i].fn_oid == InvalidOid &&
			state->triConsistentFn[i].fn_oid == InvalidOid)
		{
			elog(ERROR, "missing VODKA support function (%d or %d) for attribute %d of index \"%s\"",
					VODKA_CONSISTENT_PROC, VODKA_TRICONSISTENT_PROC,
				 i + 1, RelationGetRelationName(index));
		}

		/*
		 * If the index column has a specified collation, we should honor that
		 * while doing comparisons.  However, we may have a collatable storage
		 * type for a noncollatable indexed data type (for instance, hstore
		 * uses text index entries).  If there's no index collation then
		 * specify default collation in case the support functions need
		 * collation.  This is harmless if the support functions don't care
		 * about collation, so we just do it unconditionally.  (We could
		 * alternatively call get_typcollation, but that seems like expensive
		 * overkill --- there aren't going to be any cases where a VODKA storage
		 * type has a nondefault collation.)
		 */
		if (OidIsValid(index->rd_indcollation[i]))
			state->supportCollation[i] = index->rd_indcollation[i];
		else
			state->supportCollation[i] = DEFAULT_COLLATION_OID;

		FunctionCall2(&state->configFn[i],
			PointerGetDatum(&configIn),
			PointerGetDatum(&configOut));
	}
	initEntryIndex(state, &configOut);
	return state;
}

void
freeVodkaState(VodkaState *state)
{
	//RelationCloseSmgr(&state->entryTree);
}

/*
 * Extract attribute (column) number of stored entry from VODKA tuple
 */
OffsetNumber
vodkatuple_get_attrnum(VodkaState *vodkastate, IndexTuple tuple)
{
	OffsetNumber colN;

	if (vodkastate->oneCol)
	{
		/* column number is not stored explicitly */
		colN = FirstOffsetNumber;
	}
	else
	{
		Datum		res;
		bool		isnull;

		/*
		 * First attribute is always int16, so we can safely use any tuple
		 * descriptor to obtain first attribute of tuple
		 */
		res = index_getattr(tuple, FirstOffsetNumber, vodkastate->tupdesc[0],
							&isnull);
		Assert(!isnull);

		colN = DatumGetUInt16(res);
		Assert(colN >= FirstOffsetNumber && colN <= vodkastate->origTupdesc->natts);
	}

	return colN;
}

/*
 * Extract stored datum (and possible null category) from VODKA tuple
 */
Datum
vodkatuple_get_key(VodkaState *vodkastate, IndexTuple tuple,
				 VodkaNullCategory *category)
{
	Datum		res;
	bool		isnull;

	if (vodkastate->oneCol)
	{
		/*
		 * Single column index doesn't store attribute numbers in tuples
		 */
		res = index_getattr(tuple, FirstOffsetNumber, vodkastate->origTupdesc,
							&isnull);
	}
	else
	{
		/*
		 * Since the datum type depends on which index column it's from, we
		 * must be careful to use the right tuple descriptor here.
		 */
		OffsetNumber colN = vodkatuple_get_attrnum(vodkastate, tuple);

		res = index_getattr(tuple, OffsetNumberNext(FirstOffsetNumber),
							vodkastate->tupdesc[colN - 1],
							&isnull);
	}

	if (isnull)
		*category = VodkaGetNullCategory(tuple, vodkastate);
	else
		*category = VODKA_CAT_NORM_KEY;

	return res;
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling VodkaInitBuffer
 */
Buffer
VodkaNewBuffer(Relation index)
{
	Buffer		buffer;
	bool		needLock;

	/* First, try to get a page from FSM */
	for (;;)
	{
		BlockNumber blkno = GetFreeIndexPage(index);

		if (blkno == InvalidBlockNumber)
			break;

		buffer = ReadBuffer(index, blkno);

		/*
		 * We have to guard against the possibility that someone else already
		 * recycled this page; the buffer may be locked if so.
		 */
		if (ConditionalLockBuffer(buffer))
		{
			Page		page = BufferGetPage(buffer);

			if (PageIsNew(page))
				return buffer;	/* OK to use, if never initialized */

			if (VodkaPageIsDeleted(page))
				return buffer;	/* OK to use */

			LockBuffer(buffer, VODKA_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* Must extend the file */
	needLock = !RELATION_IS_LOCAL(index);
	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);

	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, VODKA_EXCLUSIVE);

	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	return buffer;
}

void
VodkaInitPage(Page page, uint32 f, Size pageSize)
{
	VodkaPageOpaque opaque;

	PageInit(page, pageSize, sizeof(VodkaPageOpaqueData));

	opaque = VodkaPageGetOpaque(page);
	memset(opaque, 0, sizeof(VodkaPageOpaqueData));
	opaque->flags = f;
	opaque->rightlink = InvalidBlockNumber;
}

void
VodkaInitBuffer(Buffer b, uint32 f)
{
	VodkaInitPage(BufferGetPage(b), f, BufferGetPageSize(b));
}

void
VodkaInitMetabuffer(Relation index, Buffer b, Oid relNode)
{
	VodkaMetaPageData *metadata;
	Page		page = BufferGetPage(b);

	VodkaInitPage(page, VODKA_META, BufferGetPageSize(b));

	metadata = VodkaPageGetMeta(page);

	metadata->head = metadata->tail = InvalidBlockNumber;
	metadata->tailFreeSize = 0;
	metadata->nPendingPages = 0;
	metadata->nPendingHeapTuples = 0;
	metadata->nTotalPages = 0;
	metadata->nEntryPages = 0;
	metadata->nDataPages = 0;
	metadata->nEntries = 0;
	metadata->vodkaVersion = VODKA_CURRENT_VERSION;
	metadata->entryTreeNode.dbNode = index->rd_node.dbNode;
	metadata->entryTreeNode.spcNode = index->rd_node.spcNode;
	metadata->entryTreeNode.relNode = relNode;
	metadata->postingListLUP.blkno = InvalidBlockNumber;
	metadata->postingListLUP.freeSpace = 0;
}

/*
 * Compare two keys of the same index column
 */
int
vodkaCompareEntries(VodkaState *vodkastate, OffsetNumber attnum,
				  Datum a, VodkaNullCategory categorya,
				  Datum b, VodkaNullCategory categoryb)
{
	/* if not of same null category, sort by that first */
	if (categorya != categoryb)
		return (categorya < categoryb) ? -1 : 1;

	/* all null items in same category are equal */
	if (categorya != VODKA_CAT_NORM_KEY)
		return 0;

	/* both not null, so safe to call the compareFn */
	return DatumGetInt32(FunctionCall2Coll(&vodkastate->compareFn[attnum - 1],
									  vodkastate->supportCollation[attnum - 1],
										   a, b));
}

/*
 * Compare two keys of possibly different index columns
 */
int
vodkaCompareAttEntries(VodkaState *vodkastate,
					 OffsetNumber attnuma, Datum a, VodkaNullCategory categorya,
					 OffsetNumber attnumb, Datum b, VodkaNullCategory categoryb)
{
	/* attribute number is the first sort key */
	if (attnuma != attnumb)
		return (attnuma < attnumb) ? -1 : 1;

	return vodkaCompareEntries(vodkastate, attnuma, a, categorya, b, categoryb);
}


/*
 * Support for sorting key datums in vodkaExtractEntries
 *
 * Note: we only have to worry about null and not-null keys here;
 * vodkaExtractEntries never generates more than one placeholder null,
 * so it doesn't have to sort those.
 */
typedef struct
{
	Datum		datum;
	bool		isnull;
} keyEntryData;

typedef struct
{
	FmgrInfo   *cmpDatumFunc;
	Oid			collation;
	bool		haveDups;
} cmpEntriesArg;

static int
cmpEntries(const void *a, const void *b, void *arg)
{
	const keyEntryData *aa = (const keyEntryData *) a;
	const keyEntryData *bb = (const keyEntryData *) b;
	cmpEntriesArg *data = (cmpEntriesArg *) arg;
	int			res;

	if (aa->isnull)
	{
		if (bb->isnull)
			res = 0;			/* NULL "=" NULL */
		else
			res = 1;			/* NULL ">" not-NULL */
	}
	else if (bb->isnull)
		res = -1;				/* not-NULL "<" NULL */
	else
		res = DatumGetInt32(FunctionCall2Coll(data->cmpDatumFunc,
											  data->collation,
											  aa->datum, bb->datum));

	/*
	 * Detect if we have any duplicates.  If there are equal keys, qsort must
	 * compare them at some point, else it wouldn't know whether one should go
	 * before or after the other.
	 */
	if (res == 0)
		data->haveDups = true;

	return res;
}


/*
 * Extract the index key values from an indexable item
 *
 * The resulting key values are sorted, and any duplicates are removed.
 * This avoids generating redundant index entries.
 */
Datum *
vodkaExtractEntries(VodkaState *vodkastate, OffsetNumber attnum,
				  Datum value, bool isNull,
				  int32 *nentries, VodkaNullCategory **categories)
{
	Datum	   *entries;
	bool	   *nullFlags;
	int32		i;

	/*
	 * We don't call the extractValueFn on a null item.  Instead generate a
	 * placeholder.
	 */
	if (isNull)
	{
		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		entries[0] = (Datum) 0;
		*categories = (VodkaNullCategory *) palloc(sizeof(VodkaNullCategory));
		(*categories)[0] = VODKA_CAT_NULL_ITEM;
		return entries;
	}

	/* OK, call the opclass's extractValueFn */
	nullFlags = NULL;			/* in case extractValue doesn't set it */
	entries = (Datum *)
		DatumGetPointer(FunctionCall3Coll(&vodkastate->extractValueFn[attnum - 1],
									  vodkastate->supportCollation[attnum - 1],
										  value,
										  PointerGetDatum(nentries),
										  PointerGetDatum(&nullFlags)));

	/*
	 * Generate a placeholder if the item contained no keys.
	 */
	if (entries == NULL || *nentries <= 0)
	{
		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		entries[0] = (Datum) 0;
		*categories = (VodkaNullCategory *) palloc(sizeof(VodkaNullCategory));
		(*categories)[0] = VODKA_CAT_EMPTY_ITEM;
		return entries;
	}

	/*
	 * If the extractValueFn didn't create a nullFlags array, create one,
	 * assuming that everything's non-null.  Otherwise, run through the array
	 * and make sure each value is exactly 0 or 1; this ensures binary
	 * compatibility with the VodkaNullCategory representation.
	 */
	if (nullFlags == NULL)
		nullFlags = (bool *) palloc0(*nentries * sizeof(bool));
	else
	{
		for (i = 0; i < *nentries; i++)
			nullFlags[i] = (nullFlags[i] ? true : false);
	}
	/* now we can use the nullFlags as category codes */
	*categories = (VodkaNullCategory *) nullFlags;

	/*
	 * If there's more than one key, sort and unique-ify.
	 *
	 * XXX Using qsort here is notationally painful, and the overhead is
	 * pretty bad too.	For small numbers of keys it'd likely be better to use
	 * a simple insertion sort.
	 */
	if (*nentries > 1)
	{
		keyEntryData *keydata;
		cmpEntriesArg arg;

		keydata = (keyEntryData *) palloc(*nentries * sizeof(keyEntryData));
		for (i = 0; i < *nentries; i++)
		{
			keydata[i].datum = entries[i];
			keydata[i].isnull = nullFlags[i];
		}

		arg.cmpDatumFunc = &vodkastate->compareFn[attnum - 1];
		arg.collation = vodkastate->supportCollation[attnum - 1];
		arg.haveDups = false;
		qsort_arg(keydata, *nentries, sizeof(keyEntryData),
				  cmpEntries, (void *) &arg);

		if (arg.haveDups)
		{
			/* there are duplicates, must get rid of 'em */
			int32		j;

			entries[0] = keydata[0].datum;
			nullFlags[0] = keydata[0].isnull;
			j = 1;
			for (i = 1; i < *nentries; i++)
			{
				if (cmpEntries(&keydata[i - 1], &keydata[i], &arg) != 0)
				{
					entries[j] = keydata[i].datum;
					nullFlags[j] = keydata[i].isnull;
					j++;
				}
			}
			*nentries = j;
		}
		else
		{
			/* easy, no duplicates */
			for (i = 0; i < *nentries; i++)
			{
				entries[i] = keydata[i].datum;
				nullFlags[i] = keydata[i].isnull;
			}
		}

		pfree(keydata);
	}

	return entries;
}

Datum
vodkaoptions(PG_FUNCTION_ARGS)
{
	Datum		reloptions = PG_GETARG_DATUM(0);
	bool		validate = PG_GETARG_BOOL(1);
	relopt_value *options;
	VodkaOptions *rdopts;
	int			numoptions;
	static const relopt_parse_elt tab[] = {
		{"fastupdate", RELOPT_TYPE_BOOL, offsetof(VodkaOptions, useFastUpdate)}
	};

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_VODKA,
							  &numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		PG_RETURN_NULL();

	rdopts = allocateReloptStruct(sizeof(VodkaOptions), options, numoptions);

	fillRelOptions((void *) rdopts, sizeof(VodkaOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	pfree(options);

	PG_RETURN_BYTEA_P(rdopts);
}

/*
 * Fetch index's statistical data into *stats
 *
 * Note: in the result, nPendingPages can be trusted to be up-to-date,
 * as can vodkaVersion; but the other fields are as of the last VACUUM.
 */
void
vodkaGetStats(Relation index, VodkaStatsData *stats)
{
	Buffer		metabuffer;
	Page		metapage;
	VodkaMetaPageData *metadata;

	metabuffer = ReadBuffer(index, VODKA_METAPAGE_BLKNO);
	LockBuffer(metabuffer, VODKA_SHARE);
	metapage = BufferGetPage(metabuffer);
	metadata = VodkaPageGetMeta(metapage);

	stats->nPendingPages = metadata->nPendingPages;
	stats->nTotalPages = metadata->nTotalPages;
	stats->nEntryPages = metadata->nEntryPages;
	stats->nDataPages = metadata->nDataPages;
	stats->nEntries = metadata->nEntries;
	stats->vodkaVersion = metadata->vodkaVersion;
	stats->entryTreeNode = metadata->entryTreeNode;
	stats->postingListLUP = metadata->postingListLUP;

	UnlockReleaseBuffer(metabuffer);
}

/*
 * Write the given statistics to the index's metapage
 *
 * Note: nPendingPages and vodkaVersion are *not* copied over
 */
void
vodkaUpdateStats(Relation index, const VodkaStatsData *stats)
{
	Buffer		metabuffer;
	Page		metapage;
	VodkaMetaPageData *metadata;

	metabuffer = ReadBuffer(index, VODKA_METAPAGE_BLKNO);
	LockBuffer(metabuffer, VODKA_EXCLUSIVE);
	metapage = BufferGetPage(metabuffer);
	metadata = VodkaPageGetMeta(metapage);

	START_CRIT_SECTION();

	metadata->nTotalPages = stats->nTotalPages;
	metadata->nEntryPages = stats->nEntryPages;
	metadata->nDataPages = stats->nDataPages;
	metadata->nEntries = stats->nEntries;
	metadata->postingListLUP = stats->postingListLUP;

	MarkBufferDirty(metabuffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;
		vodkaxlogUpdateMeta data;
		XLogRecData rdata;

		data.node = index->rd_node;
		data.ntuples = 0;
		data.newRightlink = data.prevTail = InvalidBlockNumber;
		memcpy(&data.metadata, metadata, sizeof(VodkaMetaPageData));

		rdata.buffer = InvalidBuffer;
		rdata.data = (char *) &data;
		rdata.len = sizeof(vodkaxlogUpdateMeta);
		rdata.next = NULL;

		recptr = XLogInsert(RM_VODKA_ID, XLOG_VODKA_UPDATE_META_PAGE, &rdata);
		PageSetLSN(metapage, recptr);
	}

	UnlockReleaseBuffer(metabuffer);

	END_CRIT_SECTION();
}
