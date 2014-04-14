/*--------------------------------------------------------------------------
 * vodka_private.h
 *	  header file for postgres inverted index access method implementation.
 *
 *	Copyright (c) 2006-2014, PostgreSQL Global Development Group
 *
 *	src/include/access/vodka_private.h
 *--------------------------------------------------------------------------
 */
#ifndef VODKA_PRIVATE_H
#define VODKA_PRIVATE_H

#include "access/genam.h"
#include "access/vodka.h"
#include "access/itup.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "utils/rbtree.h"
#include "utils/rel.h"


/*
 * Page opaque data in an inverted index page.
 *
 * Note: VODKA does not include a page ID word as do the other index types.
 * This is OK because the opaque data is only 8 bytes and so can be reliably
 * distinguished by size.  Revisit this if the size ever increases.
 * Further note: as of 9.2, SP-GiST also uses 8-byte special space.  This is
 * still OK, as long as VODKA isn't using all of the high-order bits in its
 * flags word, because that way the flags word cannot match the page ID used
 * by SP-GiST.
 */
typedef struct VodkaPageOpaqueData
{
	BlockNumber rightlink;		/* next page if any */
	OffsetNumber maxoff;		/* number of PostingItems on VODKA_DATA & ~VODKA_LEAF page.
								 * On VODKA_LIST page, number of heap tuples. */
	uint16		flags;			/* see bit definitions below */
} VodkaPageOpaqueData;

typedef VodkaPageOpaqueData *VodkaPageOpaque;

#define VODKA_DATA		  (1 << 0)
#define VODKA_LEAF		  (1 << 1)
#define VODKA_DELETED		  (1 << 2)
#define VODKA_META		  (1 << 3)
#define VODKA_LIST		  (1 << 4)
#define VODKA_LIST_FULLROW  (1 << 5)		/* makes sense only on VODKA_LIST page */
#define VODKA_INCOMPLETE_SPLIT (1 << 6)	/* page was split, but parent not updated */
#define VODKA_COMPRESSED	  (1 << 7)

/* Page numbers of fixed-location pages */
#define VODKA_METAPAGE_BLKNO	(0)
#define VODKA_ROOT_BLKNO		(1)

typedef struct VodkaMetaPageData
{
	/*
	 * Pointers to head and tail of pending list, which consists of VODKA_LIST
	 * pages.  These store fast-inserted entries that haven't yet been moved
	 * into the regular VODKA structure.
	 */
	BlockNumber head;
	BlockNumber tail;

	/*
	 * Free space in bytes in the pending list's tail page.
	 */
	uint32		tailFreeSize;

	/*
	 * We store both number of pages and number of heap tuples that are in the
	 * pending list.
	 */
	BlockNumber nPendingPages;
	int64		nPendingHeapTuples;

	/*
	 * Statistics for planner use (accurate as of last VACUUM)
	 */
	BlockNumber nTotalPages;
	BlockNumber nEntryPages;
	BlockNumber nDataPages;
	int64		nEntries;

	/*
	 * VODKA version number (ideally this should have been at the front, but too
	 * late now.  Don't move it!)
	 *
	 * Currently 2 (for indexes initialized in 9.4 or later)
	 *
	 * Version 1 (indexes initialized in version 9.1, 9.2 or 9.3), is
	 * compatible, but may contain uncompressed posting tree (leaf) pages and
	 * posting lists. They will be converted to compressed format when
	 * modified.
	 *
	 * Version 0 (indexes initialized in 9.0 or before) is compatible but may
	 * be missing null entries, including both null keys and placeholders.
	 * Reject full-index-scan attempts on such indexes.
	 */
	int32		vodkaVersion;

	RelFileNode	entryTreeNode;
} VodkaMetaPageData;

#define VODKA_CURRENT_VERSION		2

#define VodkaPageGetMeta(p) \
	((VodkaMetaPageData *) PageGetContents(p))

/*
 * Macros for accessing a VODKA index page's opaque data
 */
#define VodkaPageGetOpaque(page) ( (VodkaPageOpaque) PageGetSpecialPointer(page) )

#define VodkaPageIsLeaf(page)    ( VodkaPageGetOpaque(page)->flags & VODKA_LEAF )
#define VodkaPageSetLeaf(page)   ( VodkaPageGetOpaque(page)->flags |= VODKA_LEAF )
#define VodkaPageSetNonLeaf(page)    ( VodkaPageGetOpaque(page)->flags &= ~VODKA_LEAF )
#define VodkaPageIsData(page)    ( VodkaPageGetOpaque(page)->flags & VODKA_DATA )
#define VodkaPageSetData(page)   ( VodkaPageGetOpaque(page)->flags |= VODKA_DATA )
#define VodkaPageIsList(page)    ( VodkaPageGetOpaque(page)->flags & VODKA_LIST )
#define VodkaPageSetList(page)   ( VodkaPageGetOpaque(page)->flags |= VODKA_LIST )
#define VodkaPageHasFullRow(page)    ( VodkaPageGetOpaque(page)->flags & VODKA_LIST_FULLROW )
#define VodkaPageSetFullRow(page)   ( VodkaPageGetOpaque(page)->flags |= VODKA_LIST_FULLROW )
#define VodkaPageIsCompressed(page)    ( VodkaPageGetOpaque(page)->flags & VODKA_COMPRESSED )
#define VodkaPageSetCompressed(page)   ( VodkaPageGetOpaque(page)->flags |= VODKA_COMPRESSED )

#define VodkaPageIsDeleted(page) ( VodkaPageGetOpaque(page)->flags & VODKA_DELETED)
#define VodkaPageSetDeleted(page)    ( VodkaPageGetOpaque(page)->flags |= VODKA_DELETED)
#define VodkaPageSetNonDeleted(page) ( VodkaPageGetOpaque(page)->flags &= ~VODKA_DELETED)
#define VodkaPageIsIncompleteSplit(page) ( VodkaPageGetOpaque(page)->flags & VODKA_INCOMPLETE_SPLIT)

#define VodkaPageRightMost(page) ( VodkaPageGetOpaque(page)->rightlink == InvalidBlockNumber)

/*
 * We use our own ItemPointerGet(BlockNumber|GetOffsetNumber)
 * to avoid Asserts, since sometimes the ip_posid isn't "valid"
 */
#define VodkaItemPointerGetBlockNumber(pointer) \
	BlockIdGetBlockNumber(&(pointer)->ip_blkid)

#define VodkaItemPointerGetOffsetNumber(pointer) \
	((pointer)->ip_posid)

/*
 * Special-case item pointer values needed by the VODKA search logic.
 *	MIN: sorts less than any valid item pointer
 *	MAX: sorts greater than any valid item pointer
 *	LOSSY PAGE: indicates a whole heap page, sorts after normal item
 *				pointers for that page
 * Note that these are all distinguishable from an "invalid" item pointer
 * (which is InvalidBlockNumber/0) as well as from all normal item
 * pointers (which have item numbers in the range 1..MaxHeapTuplesPerPage).
 */
#define ItemPointerSetMin(p)  \
	ItemPointerSet((p), (BlockNumber)0, (OffsetNumber)0)
#define ItemPointerIsMin(p)  \
	(VodkaItemPointerGetOffsetNumber(p) == (OffsetNumber)0 && \
	 VodkaItemPointerGetBlockNumber(p) == (BlockNumber)0)
#define ItemPointerSetMax(p)  \
	ItemPointerSet((p), InvalidBlockNumber, (OffsetNumber)0xffff)
#define ItemPointerIsMax(p)  \
	(VodkaItemPointerGetOffsetNumber(p) == (OffsetNumber)0xffff && \
	 VodkaItemPointerGetBlockNumber(p) == InvalidBlockNumber)
#define ItemPointerSetLossyPage(p, b)  \
	ItemPointerSet((p), (b), (OffsetNumber)0xffff)
#define ItemPointerIsLossyPage(p)  \
	(VodkaItemPointerGetOffsetNumber(p) == (OffsetNumber)0xffff && \
	 VodkaItemPointerGetBlockNumber(p) != InvalidBlockNumber)

/*
 * Posting item in a non-leaf posting-tree page
 */
typedef struct
{
	/* We use BlockIdData not BlockNumber to avoid padding space wastage */
	BlockIdData child_blkno;
	ItemPointerData key;
} PostingItem;

#define PostingItemGetBlockNumber(pointer) \
	BlockIdGetBlockNumber(&(pointer)->child_blkno)

#define PostingItemSetBlockNumber(pointer, blockNumber) \
	BlockIdSet(&((pointer)->child_blkno), (blockNumber))

/*
 * Category codes to distinguish placeholder nulls from ordinary NULL keys.
 * Note that the datatype size and the first two code values are chosen to be
 * compatible with the usual usage of bool isNull flags.
 *
 * VODKA_CAT_EMPTY_QUERY is never stored in the index; and notice that it is
 * chosen to sort before not after regular key values.
 */
typedef signed char VodkaNullCategory;

#define VODKA_CAT_NORM_KEY		0		/* normal, non-null key value */
#define VODKA_CAT_NULL_KEY		1		/* null key value */
#define VODKA_CAT_EMPTY_ITEM		2		/* placeholder for zero-key item */
#define VODKA_CAT_NULL_ITEM		3		/* placeholder for null item */
#define VODKA_CAT_EMPTY_QUERY		(-1)	/* placeholder for full-scan query */

/*
 * Access macros for null category byte in entry tuples
 */
#define VodkaCategoryOffset(itup,vodkastate) \
	(IndexInfoFindDataOffset((itup)->t_info) + \
	 ((vodkastate)->oneCol ? 0 : sizeof(int16)))
#define VodkaGetNullCategory(itup,vodkastate) \
	(*((VodkaNullCategory *) ((char*)(itup) + VodkaCategoryOffset(itup,vodkastate))))
#define VodkaSetNullCategory(itup,vodkastate,c) \
	(*((VodkaNullCategory *) ((char*)(itup) + VodkaCategoryOffset(itup,vodkastate))) = (c))

/*
 * Access macros for leaf-page entry tuples (see discussion in README)
 */
#define VodkaGetNPosting(itup)	VodkaItemPointerGetOffsetNumber(&(itup)->t_tid)
#define VodkaSetNPosting(itup,n)	ItemPointerSetOffsetNumber(&(itup)->t_tid,n)
#define VODKA_TREE_POSTING		((OffsetNumber)0xffff)
#define VodkaIsPostingTree(itup)	(VodkaGetNPosting(itup) == VODKA_TREE_POSTING)
#define VodkaSetPostingTree(itup, blkno)	( VodkaSetNPosting((itup),VODKA_TREE_POSTING), ItemPointerSetBlockNumber(&(itup)->t_tid, blkno) )
#define VodkaGetPostingTree(itup) VodkaItemPointerGetBlockNumber(&(itup)->t_tid)

#define VODKA_ITUP_COMPRESSED		(1 << 31)
#define VodkaGetPostingOffset(itup)	(VodkaItemPointerGetBlockNumber(&(itup)->t_tid) & (~VODKA_ITUP_COMPRESSED))
#define VodkaSetPostingOffset(itup,n) ItemPointerSetBlockNumber(&(itup)->t_tid,(n)|VODKA_ITUP_COMPRESSED)
#define VodkaGetPosting(itup)			((Pointer) ((char*)(itup) + VodkaGetPostingOffset(itup)))
#define VodkaItupIsCompressed(itup)	(VodkaItemPointerGetBlockNumber(&(itup)->t_tid) & VODKA_ITUP_COMPRESSED)

#define VodkaMaxItemSize \
	Min(INDEX_SIZE_MASK, \
		MAXALIGN_DOWN(((BLCKSZ - SizeOfPageHeaderData -					\
						MAXALIGN(sizeof(VodkaPageOpaqueData))) / 6 - sizeof(ItemIdData))))

/*
 * Access macros for non-leaf entry tuples
 */
#define VodkaGetDownlink(itup)	VodkaItemPointerGetBlockNumber(&(itup)->t_tid)
#define VodkaSetDownlink(itup,blkno)	ItemPointerSet(&(itup)->t_tid, blkno, InvalidOffsetNumber)


/*
 * Data (posting tree) pages
 *
 * Posting tree pages don't store regular tuples. Non-leaf pages contain
 * PostingItems, which are pairs of ItemPointers and child block numbers.
 * Leaf pages contain VodkaPostingLists and an uncompressed array of item
 * pointers.
 *
 * In a leaf page, the compressed posting lists are stored after the regular
 * page header, one after each other. Although we don't store regular tuples,
 * pd_lower is used to indicate the end of the posting lists. After that, free
 * space follows.  This layout is compatible with the "standard" heap and
 * index page layout described in bufpage.h, so that we can e.g set buffer_std
 * when writing WAL records.
 *
 * In the special space is the VodkaPageOpaque struct.
 */
#define VodkaDataLeafPageGetPostingList(page) \
	(VodkaPostingList *) ((PageGetContents(page) + MAXALIGN(sizeof(ItemPointerData))))
#define VodkaDataLeafPageGetPostingListSize(page) \
	(((PageHeader) page)->pd_lower - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ItemPointerData)))
#define VodkaDataLeafPageSetPostingListSize(page, size) \
	{ \
		Assert(size <= VodkaDataLeafMaxContentSize); \
		((PageHeader) page)->pd_lower = (size) + MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(sizeof(ItemPointerData)); \
	}

#define VodkaDataLeafPageIsEmpty(page) \
	(VodkaPageIsCompressed(page) ? (VodkaDataLeafPageGetPostingListSize(page) == 0) : (VodkaPageGetOpaque(page)->maxoff < FirstOffsetNumber))

#define VodkaDataLeafPageGetFreeSpace(page) PageGetExactFreeSpace(page)

#define VodkaDataPageGetRightBound(page)	((ItemPointer) PageGetContents(page))
/*
 * Pointer to the data portion of a posting tree page. For internal pages,
 * that's the beginning of the array of PostingItems. For compressed leaf
 * pages, the first compressed posting list. For uncompressed (pre-9.4) leaf
 * pages, it's the beginning of the ItemPointer array.
 */
#define VodkaDataPageGetData(page)	\
	(PageGetContents(page) + MAXALIGN(sizeof(ItemPointerData)))
/* non-leaf pages contain PostingItems */
#define VodkaDataPageGetPostingItem(page, i)	\
	((PostingItem *) (VodkaDataPageGetData(page) + ((i)-1) * sizeof(PostingItem)))

#define VodkaNonLeafDataPageGetFreeSpace(page)	\
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	 - MAXALIGN(sizeof(ItemPointerData)) \
	 - VodkaPageGetOpaque(page)->maxoff * sizeof(PostingItem)	\
	 - MAXALIGN(sizeof(VodkaPageOpaqueData)))

#define VodkaDataLeafMaxContentSize	\
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	 - MAXALIGN(sizeof(ItemPointerData)) \
	 - MAXALIGN(sizeof(VodkaPageOpaqueData)))

/*
 * List pages
 */
#define VodkaListPageSize  \
	( BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(VodkaPageOpaqueData)) )

/*
 * Storage type for VODKA's reloptions
 */
typedef struct VodkaOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	bool		useFastUpdate;	/* use fast updates? */
} VodkaOptions;

#define VODKA_DEFAULT_USE_FASTUPDATE	true
#define VodkaGetUseFastUpdate(relation) \
	((relation)->rd_options ? \
	 ((VodkaOptions *) (relation)->rd_options)->useFastUpdate : VODKA_DEFAULT_USE_FASTUPDATE)


/* Macros for buffer lock/unlock operations */
#define VODKA_UNLOCK	BUFFER_LOCK_UNLOCK
#define VODKA_SHARE	BUFFER_LOCK_SHARE
#define VODKA_EXCLUSIVE  BUFFER_LOCK_EXCLUSIVE


/*
 * VodkaState: working data structure describing the index being worked on
 */
typedef struct VodkaState
{
	Relation	index;
	bool		oneCol;			/* true if single-column index */

	/*
	 * origTupDesc is the nominal tuple descriptor of the index, ie, the i'th
	 * attribute shows the key type (not the input data type!) of the i'th
	 * index column.  In a single-column index this describes the actual leaf
	 * index tuples.  In a multi-column index, the actual leaf tuples contain
	 * a smallint column number followed by a key datum of the appropriate
	 * type for that column.  We set up tupdesc[i] to describe the actual
	 * rowtype of the index tuples for the i'th column, ie, (int2, keytype).
	 * Note that in any case, leaf tuples contain more data than is known to
	 * the TupleDesc; see access/vodka/README for details.
	 */
	TupleDesc	origTupdesc;
	TupleDesc	tupdesc[INDEX_MAX_KEYS];

	/*
	 * Per-index-column opclass support functions
	 */
	FmgrInfo	configFn[INDEX_MAX_KEYS];
	FmgrInfo	compareFn[INDEX_MAX_KEYS];
	FmgrInfo	extractValueFn[INDEX_MAX_KEYS];
	FmgrInfo	extractQueryFn[INDEX_MAX_KEYS];
	FmgrInfo	consistentFn[INDEX_MAX_KEYS];
	FmgrInfo	triConsistentFn[INDEX_MAX_KEYS];
	/* Collations to pass to the support functions */
	Oid			supportCollation[INDEX_MAX_KEYS];

	RelFileNode	entryTreeNode;
	RelationData entryTree;
	Oid			entryTreeOpFamily;
	IndexScanDesc	entryEqualScan;
	Oid			entryEqualOperator;
} VodkaState;


/*
 * A compressed posting list.
 *
 * Note: This requires 2-byte alignment.
 */
typedef struct
{
	ItemPointerData first;	/* first item in this posting list (unpacked) */
	uint16		nbytes;		/* number of bytes that follow */
	unsigned char bytes[1];	/* varbyte encoded items (variable length) */
} VodkaPostingList;

#define SizeOfVodkaPostingList(plist) (offsetof(VodkaPostingList, bytes) + SHORTALIGN((plist)->nbytes) )
#define VodkaNextPostingListSegment(cur) ((VodkaPostingList *) (((char *) (cur)) + SizeOfVodkaPostingList((cur))))


/* XLog stuff */

#define XLOG_VODKA_CREATE_INDEX  0x00

#define XLOG_VODKA_CREATE_PTREE  0x10

typedef struct vodkaxlogCreatePostingTree
{
	RelFileNode node;
	BlockNumber blkno;
	uint32		size;
	/* A compressed posting list follows */
} vodkaxlogCreatePostingTree;

#define XLOG_VODKA_INSERT  0x20

/*
 * The format of the insertion record varies depending on the page type.
 * vodkaxlogInsert is the common part between all variants.
 */
typedef struct
{
	RelFileNode node;
	BlockNumber blkno;
	uint16		flags;		/* VODKA_SPLIT_ISLEAF and/or VODKA_SPLIT_ISDATA */

	/*
	 * FOLLOWS:
	 *
	 * 1. if not leaf page, block numbers of the left and right child pages
	 * whose split this insertion finishes. As BlockIdData[2] (beware of adding
	 * fields before this that would make them not 16-bit aligned)
	 *
	 * 2. one of the following structs, depending on tree type.
	 *
	 * NB: the below structs are only 16-bit aligned when appended to a
	 * vodkaxlogInsert struct! Beware of adding fields to them that require
	 * stricter alignment.
	 */
} vodkaxlogInsert;

typedef struct
{
	OffsetNumber offset;
	bool		isDelete;
	IndexTupleData tuple;	/* variable length */
} vodkaxlogInsertEntry;

typedef struct
{
      uint16          nactions;

      /* Variable number of 'actions' follow */
} vodkaxlogRecompressDataLeaf;

/*
 * Note: this struct is currently not used in code, and only acts as
 * documentation. The WAL record format is as specified here, but the code
 * uses straight access through a Pointer and memcpy to read/write these.
 */
typedef struct
{
      uint8           segno;          /* segment this action applies to */
      char            type;           /* action type (see below) */

      /*
       * Action-specific data follows. For INSERT and REPLACE actions that is a
       * GinPostingList struct. For ADDITEMS, a uint16 for the number of items
       * added, followed by the items themselves as ItemPointers. DELETE actions
       * have no further data.
       */
} vodkaxlogSegmentAction;

/* Action types */
#define VODKA_SEGMENT_UNMODIFIED        0       /* no action (not used in WAL records) */
#define VODKA_SEGMENT_DELETE            1       /* a whole segment is removed */
#define VODKA_SEGMENT_INSERT            2       /* a whole segment is added */
#define VODKA_SEGMENT_REPLACE           3       /* a segment is replaced */
#define VODKA_SEGMENT_ADDITEMS  4       /* items are added to existing segment */

typedef struct
{
	OffsetNumber offset;
	PostingItem newitem;
} vodkaxlogInsertDataInternal;


#define XLOG_VODKA_SPLIT	0x30

typedef struct vodkaxlogSplit
{
	RelFileNode node;
	BlockNumber lblkno;
	BlockNumber rblkno;
	BlockNumber rrlink;				/* right link, or root's blocknumber if root split */
	BlockNumber	leftChildBlkno;		/* valid on a non-leaf split */
	BlockNumber	rightChildBlkno;
	uint16		flags;

	/* follows: one of the following structs */
} vodkaxlogSplit;

/*
 * Flags used in vodkaxlogInsert and vodkaxlogSplit records
 */
#define VODKA_INSERT_ISDATA	0x01	/* for both insert and split records */
#define VODKA_INSERT_ISLEAF	0x02	/* .. */
#define VODKA_SPLIT_ROOT		0x04	/* only for split records */

typedef struct
{
	OffsetNumber separator;
	OffsetNumber nitem;

	/* FOLLOWS: IndexTuples */
} vodkaxlogSplitEntry;

typedef struct
{
	uint16		lsize;
	uint16		rsize;
	ItemPointerData lrightbound;	/* new right bound of left page */
	ItemPointerData rrightbound;	/* new right bound of right page */

	/* FOLLOWS: new compressed posting lists of left and right page */
	char		newdata[1];
} vodkaxlogSplitDataLeaf;

typedef struct
{
	OffsetNumber separator;
	OffsetNumber nitem;
	ItemPointerData rightbound;

	/* FOLLOWS: array of PostingItems */
} vodkaxlogSplitDataInternal;

/*
 * Vacuum simply WAL-logs the whole page, when anything is modified. This
 * functionally identical heap_newpage records, but is kept separate for
 * debugvodkag purposes. (When inspecting the WAL stream, it's easier to see
 * what's going on when VODKA vacuum records are marked as such, not as heap
 * records.) This is currently only used for entry tree leaf pages.
 */
#define XLOG_VODKA_VACUUM_PAGE	0x40

typedef struct vodkaxlogVacuumPage
{
	RelFileNode node;
	BlockNumber blkno;
	uint16		hole_offset;	/* number of bytes before "hole" */
	uint16		hole_length;	/* number of bytes in "hole" */
	/* entire page contents (minus the hole) follow at end of record */
} vodkaxlogVacuumPage;

/*
 * Vacuuming posting tree leaf page is WAL-logged like recompression caused
 * by insertion.
 */
#define XLOG_VODKA_VACUUM_DATA_LEAF_PAGE	0x90

typedef struct vodkaxlogVacuumDataLeafPage
{
	RelFileNode node;
	BlockNumber blkno;

	vodkaxlogRecompressDataLeaf data;
} vodkaxlogVacuumDataLeafPage;

#define XLOG_VODKA_DELETE_PAGE	0x50

typedef struct vodkaxlogDeletePage
{
	RelFileNode node;
	BlockNumber blkno;
	BlockNumber parentBlkno;
	OffsetNumber parentOffset;
	BlockNumber leftBlkno;
	BlockNumber rightLink;
} vodkaxlogDeletePage;

#define XLOG_VODKA_UPDATE_META_PAGE 0x60

typedef struct vodkaxlogUpdateMeta
{
	RelFileNode node;
	VodkaMetaPageData metadata;
	BlockNumber prevTail;
	BlockNumber newRightlink;
	int32		ntuples;		/* if ntuples > 0 then metadata.tail was
								 * updated with that many tuples; else new sub
								 * list was inserted */
	/* array of inserted tuples follows */
} vodkaxlogUpdateMeta;

#define XLOG_VODKA_INSERT_LISTPAGE  0x70

typedef struct vodkaxlogInsertListPage
{
	RelFileNode node;
	BlockNumber blkno;
	BlockNumber rightlink;
	int32		ntuples;
	/* array of inserted tuples follows */
} vodkaxlogInsertListPage;

#define XLOG_VODKA_DELETE_LISTPAGE  0x80

#define VODKA_NDELETE_AT_ONCE 16
typedef struct vodkaxlogDeleteListPages
{
	RelFileNode node;
	VodkaMetaPageData metadata;
	int32		ndeleted;
	BlockNumber toDelete[VODKA_NDELETE_AT_ONCE];
} vodkaxlogDeleteListPages;


/* vodkautil.c */
extern Datum vodkaoptions(PG_FUNCTION_ARGS);
extern void initVodkaState(VodkaState *state, Relation index);
extern IndexScanDesc prepareEntryIndexScan(VodkaState *state, Oid operator, Datum value);
extern void freeVodkaState(VodkaState *state);
extern Buffer VodkaNewBuffer(Relation index);
extern void VodkaInitBuffer(Buffer b, uint32 f);
extern void VodkaInitPage(Page page, uint32 f, Size pageSize);
extern void VodkaInitMetabuffer(Relation index, Buffer b, Oid relnode);
extern int vodkaCompareEntries(VodkaState *vodkastate, OffsetNumber attnum,
				  Datum a, VodkaNullCategory categorya,
				  Datum b, VodkaNullCategory categoryb);
extern int vodkaCompareAttEntries(VodkaState *vodkastate,
					 OffsetNumber attnuma, Datum a, VodkaNullCategory categorya,
				   OffsetNumber attnumb, Datum b, VodkaNullCategory categoryb);
extern Datum *vodkaExtractEntries(VodkaState *vodkastate, OffsetNumber attnum,
				  Datum value, bool isNull,
				  int32 *nentries, VodkaNullCategory **categories);

extern OffsetNumber vodkatuple_get_attrnum(VodkaState *vodkastate, IndexTuple tuple);
extern Datum vodkatuple_get_key(VodkaState *vodkastate, IndexTuple tuple,
				 VodkaNullCategory *category);

/* vodkainsert.c */
extern Datum vodkabuild(PG_FUNCTION_ARGS);
extern Datum vodkabuildempty(PG_FUNCTION_ARGS);
extern Datum vodkainsert(PG_FUNCTION_ARGS);
extern void vodkaEntryInsert(VodkaState *vodkastate,
			   OffsetNumber attnum, Datum key, VodkaNullCategory category,
			   ItemPointerData *items, uint32 nitem,
			   VodkaStatsData *buildStats);

/* vodkabtree.c */

typedef struct VodkaBtreeStack
{
	BlockNumber blkno;
	Buffer		buffer;
	OffsetNumber off;
	ItemPointerData iptr;
	/* predictNumber contains predicted number of pages on current level */
	uint32		predictNumber;
	struct VodkaBtreeStack *parent;
} VodkaBtreeStack;

typedef struct VodkaBtreeData *VodkaBtree;

/* Return codes for VodkaBtreeData.placeToPage method */
typedef enum
{
	UNMODIFIED,
	INSERTED,
	SPLIT
} VodkaPlaceToPageRC;

typedef struct VodkaBtreeData
{
	/* search methods */
	BlockNumber (*findChildPage) (VodkaBtree, VodkaBtreeStack *);
	BlockNumber (*getLeftMostChild) (VodkaBtree, Page);
	bool		(*isMoveRight) (VodkaBtree, Page);
	bool		(*findItem) (VodkaBtree, VodkaBtreeStack *);

	/* insert methods */
	OffsetNumber (*findChildPtr) (VodkaBtree, Page, BlockNumber, OffsetNumber);
	VodkaPlaceToPageRC (*placeToPage) (VodkaBtree, Buffer, VodkaBtreeStack *, void *, BlockNumber, XLogRecData **, Page *, Page *);
	void	   *(*prepareDownlink) (VodkaBtree, Buffer);
	void		(*fillRoot) (VodkaBtree, Page, BlockNumber, Page, BlockNumber, Page);

	bool		isData;

	Relation	index;
	BlockNumber rootBlkno;
	VodkaState   *vodkastate;		/* not valid in a data scan */
	bool		fullScan;
	bool		isBuild;

	/* Search key for Entry tree */
	OffsetNumber entryAttnum;
	Datum		entryKey;
	VodkaNullCategory entryCategory;

	/* Search key for data tree (posting tree) */
	ItemPointerData itemptr;
} VodkaBtreeData;

/* This represents a tuple to be inserted to entry tree. */
typedef struct
{
	IndexTuple	entry;			/* tuple to insert */
	bool		isDelete;		/* delete old tuple at same offset? */
} VodkaBtreeEntryInsertData;

/*
 * This represents an itempointer, or many itempointers, to be inserted to
 * a data (posting tree) leaf page
 */
typedef struct
{
	ItemPointerData *items;
	uint32		nitem;
	uint32		curitem;
} VodkaBtreeDataLeafInsertData;

/*
 * For internal data (posting tree) pages, the insertion payload is a
 * PostingItem
 */

extern VodkaBtreeStack *vodkaFindLeafPage(VodkaBtree btree, bool searchMode);
extern Buffer vodkaStepRight(Buffer buffer, Relation index, int lockmode);
extern void freeVodkaBtreeStack(VodkaBtreeStack *stack);
extern void vodkaInsertValue(VodkaBtree btree, VodkaBtreeStack *stack,
			   void *insertdata, VodkaStatsData *buildStats);

/* vodkaentrypage.c */
extern IndexTuple VodkaFormTuple(VodkaState *vodkastate,
			 OffsetNumber attnum, Datum key, VodkaNullCategory category,
			 Pointer data, Size dataSize, int nipd, bool errorTooBig);
extern void vodkaPrepareEntryScan(VodkaBtree btree, OffsetNumber attnum,
					Datum key, VodkaNullCategory category,
					VodkaState *vodkastate);
extern void vodkaEntryFillRoot(VodkaBtree btree, Page root, BlockNumber lblkno, Page lpage, BlockNumber rblkno, Page rpage);
extern ItemPointer vodkaReadTuple(VodkaState *vodkastate, OffsetNumber attnum,
			 IndexTuple itup, int *nitems);

/* vodkadatapage.c */
extern ItemPointer VodkaDataLeafPageGetItems(Page page, int *nitems, ItemPointerData advancePast);
extern int VodkaDataLeafPageGetItemsToTbm(Page page, TIDBitmap *tbm);
extern BlockNumber vodkaCreatePostingTree(Relation index,
				  ItemPointerData *items, uint32 nitems,
				  VodkaStatsData *buildStats);
extern void VodkaDataPageAddPostingItem(Page page, PostingItem *data, OffsetNumber offset);
extern void VodkaPageDeletePostingItem(Page page, OffsetNumber offset);
extern void vodkaInsertItemPointers(Relation index, BlockNumber rootBlkno,
					  ItemPointerData *items, uint32 nitem,
					  VodkaStatsData *buildStats);
extern VodkaBtreeStack *vodkaScanBeginPostingTree(VodkaBtree btree, Relation index, BlockNumber rootBlkno);
extern void vodkaDataFillRoot(VodkaBtree btree, Page root, BlockNumber lblkno, Page lpage, BlockNumber rblkno, Page rpage);
extern void vodkaPrepareDataScan(VodkaBtree btree, Relation index, BlockNumber rootBlkno);

/*
 * This is declared in vodkavacuum.c, but is passed between vodkaVacuumItemPointers
 * and vodkaVacuumPostingTreeLeaf and as an opaque struct, so we need a forward
 * declaration for it.
 */
typedef struct VodkaVacuumState VodkaVacuumState;

extern void vodkaVacuumPostingTreeLeaf(Relation rel, Buffer buf, VodkaVacuumState *gvs);

/* vodkascan.c */

/*
 * VodkaScanKeyData describes a single VODKA index qualifier expression.
 *
 * From each qual expression, we extract one or more specific index search
 * conditions, which are represented by VodkaScanEntryData.  It's quite
 * possible for identical search conditions to be requested by more than
 * one qual expression, in which case we merge such conditions to have just
 * one unique VodkaScanEntry --- this is particularly important for efficiency
 * when dealing with full-index-scan entries.  So there can be multiple
 * VodkaScanKeyData.scanEntry pointers to the same VodkaScanEntryData.
 *
 * In each VodkaScanKeyData, nentries is the true number of entries, while
 * nuserentries is the number that extractQueryFn returned (which is what
 * we report to consistentFn).	The "user" entries must come first.
 */
typedef struct VodkaScanKeyData *VodkaScanKey;

typedef struct VodkaScanEntryData *VodkaScanEntry;

typedef struct VodkaScanKeyData
{
	/* Real number of entries in scanEntry[] (always > 0) */
	uint32		nentries;
	/* Number of entries that extractQueryFn and consistentFn know about */
	uint32		nuserentries;

	/* array of VodkaScanEntry pointers, one per extracted search condition */
	VodkaScanEntry *scanEntry;

	/*
	 * At least one of the entries in requiredEntries must be present for
	 * a tuple to match the overall qual.
	 *
	 * additionalEntries contains entries that are needed by the consistent
	 * function to decide if an item matches, but are not sufficient to
	 * satisfy the qual without entries from requiredEntries.
	 */
	VodkaScanEntry *requiredEntries;
	int			nrequired;
	VodkaScanEntry *additionalEntries;
	int			nadditional;

	/* array of check flags, reported to consistentFn */
	bool	   *entryRes;
	bool		(*boolConsistentFn) (VodkaScanKey key);
	VodkaTernaryValue (*triConsistentFn) (VodkaScanKey key);
	FmgrInfo   *consistentFmgrInfo;
	FmgrInfo   *triConsistentFmgrInfo;
	Oid			collation;

	/* other data needed for calling consistentFn */
	Datum		query;
	/* NB: these three arrays have only nuserentries elements! */
	VodkaKey   *queryValues;
	Pointer    *extra_data;
	StrategyNumber strategy;
	int32		searchMode;
	OffsetNumber attnum;

	/*
	 * Match status data.  curItem is the TID most recently tested (could be a
	 * lossy-page pointer).  curItemMatches is TRUE if it passes the
	 * consistentFn test; if so, recheckCurItem is the recheck flag.
	 * isFinished means that all the input entry streams are finished, so this
	 * key cannot succeed for any later TIDs.
	 */
	ItemPointerData curItem;
	bool		curItemMatches;
	bool		recheckCurItem;
	bool		isFinished;
}	VodkaScanKeyData;

typedef struct VodkaScanEntryData
{
	/* query key and other information from extractQueryFn */
	Datum		queryKey;
	VodkaNullCategory queryCategory;
	Oid			operator;
	Pointer		extra_data;
	StrategyNumber strategy;
	int32		searchMode;
	OffsetNumber attnum;

	/* Current page in posting tree */
	Buffer		buffer;

	/* current ItemPointer to heap */
	ItemPointerData curItem;

	/* for a partial-match or full-scan query, we accumulate all TIDs here */
	TIDBitmap  *matchBitmap;
	TBMIterator *matchIterator;
	TBMIterateResult *matchResult;

	/* used for Posting list and one page in Posting tree */
	ItemPointerData *list;
	int			nlist;
	OffsetNumber offset;

	bool		isFinished;
	bool		reduceResult;
	uint32		predictNumberResult;
	VodkaBtreeData btree;
}	VodkaScanEntryData;

typedef struct VodkaScanOpaqueData
{
	MemoryContext tempCtx;
	VodkaState	vodkastate;

	VodkaScanKey	keys;			/* one per scan qualifier expr */
	uint32		nkeys;

	VodkaScanEntry *entries;		/* one per index search condition */
	uint32		totalentries;
	uint32		allocentries;	/* allocated length of entries[] */

	bool		isVoidRes;		/* true if query is unsatisfiable */
} VodkaScanOpaqueData;

typedef VodkaScanOpaqueData *VodkaScanOpaque;

extern Datum vodkabeginscan(PG_FUNCTION_ARGS);
extern Datum vodkaendscan(PG_FUNCTION_ARGS);
extern Datum vodkarescan(PG_FUNCTION_ARGS);
extern Datum vodkamarkpos(PG_FUNCTION_ARGS);
extern Datum vodkarestrpos(PG_FUNCTION_ARGS);
extern void vodkaNewScanKey(IndexScanDesc scan);

/* vodkaget.c */
extern Datum vodkagetbitmap(PG_FUNCTION_ARGS);

/* vodkalogic.c */
extern void vodkaInitConsistentFunction(VodkaState *vodkastate, VodkaScanKey key);

/* vodkavacuum.c */
extern Datum vodkabulkdelete(PG_FUNCTION_ARGS);
extern Datum vodkavacuumcleanup(PG_FUNCTION_ARGS);
extern ItemPointer vodkaVacuumItemPointers(VodkaVacuumState *gvs,
					  ItemPointerData *items, int nitem, int *nremaining);

/* vodkabulk.c */
typedef struct VodkaEntryAccumulator
{
	RBNode		rbnode;
	Datum		key;
	VodkaNullCategory category;
	OffsetNumber attnum;
	bool		shouldSort;
	ItemPointerData *list;
	uint32		maxcount;		/* allocated size of list[] */
	uint32		count;			/* current number of list[] entries */
} VodkaEntryAccumulator;

typedef struct
{
	VodkaState   *vodkastate;
	long		allocatedMemory;
	VodkaEntryAccumulator *entryallocator;
	uint32		eas_used;
	RBTree	   *tree;
} BuildAccumulator;

extern void vodkaInitBA(BuildAccumulator *accum);
extern void vodkaInsertBAEntries(BuildAccumulator *accum,
				   ItemPointer heapptr, OffsetNumber attnum,
				   Datum *entries, VodkaNullCategory *categories,
				   int32 nentries);
extern void vodkaBeginBAScan(BuildAccumulator *accum);
extern ItemPointerData *vodkaGetBAEntry(BuildAccumulator *accum,
			  OffsetNumber *attnum, Datum *key, VodkaNullCategory *category,
			  uint32 *n);

/* vodkafast.c */

typedef struct VodkaTupleCollector
{
	IndexTuple *tuples;
	uint32		ntuples;
	uint32		lentuples;
	uint32		sumsize;
} VodkaTupleCollector;

extern void vodkaHeapTupleFastInsert(VodkaState *vodkastate,
					   VodkaTupleCollector *collector);
extern void vodkaHeapTupleFastCollect(VodkaState *vodkastate,
						VodkaTupleCollector *collector,
						OffsetNumber attnum, Datum value, bool isNull,
						ItemPointer ht_ctid);
extern void vodkaInsertCleanup(VodkaState *vodkastate,
				 bool vac_delay, IndexBulkDeleteResult *stats);

/* vodkapostinglist.c */

extern VodkaPostingList *vodkaCompressPostingList(const ItemPointer ptrs, int nptrs,
								   int maxsize, int *nwritten);
extern int vodkaPostingListDecodeAllSegmentsToTbm(VodkaPostingList *ptr, int totalsize, TIDBitmap *tbm);

extern ItemPointer vodkaPostingListDecodeAllSegments(VodkaPostingList *ptr, int len, int *ndecoded);
extern ItemPointer vodkaPostingListDecode(VodkaPostingList *ptr, int *ndecoded);
extern ItemPointer vodkaMergeItemPointers(ItemPointerData *a, uint32 na,
					 ItemPointerData *b, uint32 nb,
					 int *nmerged);

/*
 * Mervodkag the results of several vodka scans compares item pointers a lot,
 * so we want this to be inlined. But if the compiler doesn't support that,
 * fall back on the non-inline version from itemptr.c. See STATIC_IF_INLINE in
 * c.h.
 */
#ifdef PG_USE_INLINE
static inline int
vodkaCompareItemPointers(ItemPointer a, ItemPointer b)
{
	uint64 ia = (uint64) a->ip_blkid.bi_hi << 32 | (uint64) a->ip_blkid.bi_lo << 16 | a->ip_posid;
	uint64 ib = (uint64) b->ip_blkid.bi_hi << 32 | (uint64) b->ip_blkid.bi_lo << 16 | b->ip_posid;

	if (ia == ib)
		return 0;
	else if (ia > ib)
		return 1;
	else
		return -1;
}
#else
#define vodkaCompareItemPointers(a, b) ItemPointerCompare(a, b)
#endif   /* PG_USE_INLINE */

#endif   /* VODKA_PRIVATE_H */
