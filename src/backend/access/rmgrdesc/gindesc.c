/*-------------------------------------------------------------------------
 *
 * gindesc.c
 *	  rmgr descriptor routines for access/transam/gin/ginxlog.c
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/gindesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin_private.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"

static void
desc_node(StringInfo buf, RelFileNode node, BlockNumber blkno)
{
	appendStringInfo(buf, "node: %u/%u/%u blkno: %u",
					 node.spcNode, node.dbNode, node.relNode, blkno);
}

void
gin_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_GIN_CREATE_INDEX:
			appendStringInfoString(buf, "Create index, ");
			desc_node(buf, *(RelFileNode *) rec, GIN_ROOT_BLKNO);
			break;
		case XLOG_GIN_CREATE_PTREE:
			appendStringInfoString(buf, "Create posting tree, ");
			desc_node(buf, ((ginxlogCreatePostingTree *) rec)->node, ((ginxlogCreatePostingTree *) rec)->blkno);
			break;
		case XLOG_GIN_INSERT:
			{
				ginxlogInsert *xlrec = (ginxlogInsert *) rec;
				char	*payload = rec + sizeof(ginxlogInsert);

				appendStringInfoString(buf, "Insert item, ");
				desc_node(buf, xlrec->node, xlrec->blkno);
				appendStringInfo(buf, " isdata: %c isleaf: %c",
								 (xlrec->flags & GIN_INSERT_ISDATA) ? 'T' : 'F',
								 (xlrec->flags & GIN_INSERT_ISLEAF) ? 'T' : 'F');
				if (!(xlrec->flags & GIN_INSERT_ISLEAF))
				{
					BlockNumber leftChildBlkno;
					BlockNumber rightChildBlkno;

					leftChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
					payload += sizeof(BlockIdData);
					rightChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
					payload += sizeof(BlockNumber);
					appendStringInfo(buf, " children: %u/%u",
									 leftChildBlkno, rightChildBlkno);
				}
				if (!(xlrec->flags & GIN_INSERT_ISDATA))
					appendStringInfo(buf, " isdelete: %c",
									 (((ginxlogInsertEntry *) payload)->isDelete) ? 'T' : 'F');
				else if (xlrec->flags & GIN_INSERT_ISLEAF)
				{
#ifdef BROKEN
					ginxlogInsertDataLeaf *data = (ginxlogInsertDataLeaf *) rec;
					appendStringInfo(buf, " begin %u, len %u, restOffset %u",
									 data->beginOffset,
									 data->newlen,
									 data->restOffset);
#endif
				}
				else
				{
					ginxlogInsertDataInternal *insertData = (ginxlogInsertDataInternal *) payload;
					appendStringInfo(buf, " pitem: %u-%u/%u",
									 PostingItemGetBlockNumber(&insertData->newitem),
									 ItemPointerGetBlockNumber(&insertData->newitem.key),
									 ItemPointerGetOffsetNumber(&insertData->newitem.key));
				}
			}
			break;
		case XLOG_GIN_SPLIT:
			appendStringInfoString(buf, "Page split, ");
			desc_node(buf, ((ginxlogSplit *) rec)->node, ((ginxlogSplit *) rec)->lblkno);
			appendStringInfo(buf, " isrootsplit: %c", (((ginxlogSplit *) rec)->flags & GIN_SPLIT_ROOT) ? 'T' : 'F');
			break;
		case XLOG_GIN_VACUUM_PAGE:
			appendStringInfoString(buf, "Vacuum page, ");
			desc_node(buf, ((ginxlogVacuumPage *) rec)->node, ((ginxlogVacuumPage *) rec)->blkno);
			break;
		case XLOG_GIN_DELETE_PAGE:
			appendStringInfoString(buf, "Delete page, ");
			desc_node(buf, ((ginxlogDeletePage *) rec)->node, ((ginxlogDeletePage *) rec)->blkno);
			break;
		case XLOG_GIN_UPDATE_META_PAGE:
			appendStringInfoString(buf, "Update metapage, ");
			desc_node(buf, ((ginxlogUpdateMeta *) rec)->node, GIN_METAPAGE_BLKNO);
			break;
		case XLOG_GIN_INSERT_LISTPAGE:
			appendStringInfoString(buf, "Insert new list page, ");
			desc_node(buf, ((ginxlogInsertListPage *) rec)->node, ((ginxlogInsertListPage *) rec)->blkno);
			break;
		case XLOG_GIN_DELETE_LISTPAGE:
			appendStringInfo(buf, "Delete list pages (%d), ", ((ginxlogDeleteListPages *) rec)->ndeleted);
			desc_node(buf, ((ginxlogDeleteListPages *) rec)->node, GIN_METAPAGE_BLKNO);
			break;
		default:
			appendStringInfo(buf, "unknown gin op code %u", info);
			break;
	}
}
