/*-------------------------------------------------------------------------
 *
 * gindesc.c
 *	  rmgr descriptor routines for access/transam/gin/ginxlog.c
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
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
			if (!((ginxlogInsertCommon *) rec)->isData)
			{
				ginxlogInsertEntry *data = (ginxlogInsertEntry *) rec;
				appendStringInfoString(buf, "Insert entry, ");
				desc_node(buf, data->common.node, data->common.blkno);
				appendStringInfo(buf, " offset %u, isleaf %c, isdelete %c, updateBlkno %u",
								 data->offset,
								 data->common.isLeaf ? 'T' : 'F',
								 data->isDelete ? 'T' : 'F',
								 data->common.updateBlkno);
			}
			else if (!((ginxlogInsertCommon *) rec)->isLeaf)
			{
				ginxlogInsertDataInternal *data = (ginxlogInsertDataInternal *) rec;
				appendStringInfoString(buf, "Insert non-leaf data, ");
				desc_node(buf, data->common.node, data->common.blkno);
				appendStringInfo(buf, " offset %u, child %u, key %u/%u, updateBlkno %u",
								 data->offset,
								 PostingItemGetBlockNumber(&data->newitem),
								 ItemPointerGetBlockNumber(&data->newitem.key),
								 ItemPointerGetOffsetNumber(&data->newitem.key),
								 data->common.updateBlkno);
			}
			else
			{
				ginxlogInsertDataLeaf *data = (ginxlogInsertDataLeaf *) rec;
				appendStringInfoString(buf, "Insert leaf data, ");
				desc_node(buf, data->common.node, data->common.blkno);
				appendStringInfo(buf, " begin %u, len %u, restOffset %u",
								 data->beginOffset,
								 data->newlen,
								 data->restOffset);
			}
			break;
		case XLOG_GIN_SPLIT:
			appendStringInfoString(buf, "Page split, ");
			desc_node(buf, ((ginxlogSplit *) rec)->node, ((ginxlogSplit *) rec)->lblkno);
			appendStringInfo(buf, " rblkno: %u, isrootsplit: %c",
							 ((ginxlogSplit *) rec)->rblkno,
							 (((ginxlogSplit *) rec)->isRootSplit) ? 'T' : 'F');
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
