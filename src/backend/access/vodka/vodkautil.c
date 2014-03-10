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

#include "access/vodka_private.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"


/*
 * initVodkaState: fill in an empty VodkaState struct to describe the index
 *
 * Note: assorted subsidiary data is allocated in the CurrentMemoryContext.
 */
void
initVodkaState(VodkaState *state, Relation index)
{
	TupleDesc	origTupdesc = RelationGetDescr(index);
	int			i;

	MemSet(state, 0, sizeof(VodkaState));

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

		fmgr_info_copy(&(state->compareFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_COMPARE_PROC),
					   CurrentMemoryContext);
		fmgr_info_copy(&(state->extractValueFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_EXTRACTVALUE_PROC),
					   CurrentMemoryContext);
		fmgr_info_copy(&(state->extractQueryFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_EXTRACTQUERY_PROC),
					   CurrentMemoryContext);
		fmgr_info_copy(&(state->consistentFn[i]),
					   index_getprocinfo(index, i + 1, VODKA_CONSISTENT_PROC),
					   CurrentMemoryContext);

		/*
		 * Check opclass capability to do partial match.
		 */
		if (index_getprocid(index, i + 1, VODKA_COMPARE_PARTIAL_PROC) != InvalidOid)
		{
			fmgr_info_copy(&(state->comparePartialFn[i]),
				   index_getprocinfo(index, i + 1, VODKA_COMPARE_PARTIAL_PROC),
						   CurrentMemoryContext);
			state->canPartialMatch[i] = true;
		}
		else
		{
			state->canPartialMatch[i] = false;
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
	}
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
VodkaInitMetabuffer(VodkaState *state, Buffer b)
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
	metadata->entryTreeNode.dbNode = MyDatabaseId;
	metadata->entryTreeNode.spcNode = state->index->rd_rel->reltablespace;
	metadata->entryTreeNode.relNode = GetNewRelFileNode(
			state->index->rd_rel->reltablespace,
			NULL,
			state->index->rd_rel->relpersistence);
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
