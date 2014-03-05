/*-------------------------------------------------------------------------
 *
 * vodkadesc.c
 *	  rmgr descriptor routines for access/transam/vodka/vodkaxlog.c
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/vodkadesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/vodka_private.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"

static void
desc_node(StringInfo buf, RelFileNode node, BlockNumber blkno)
{
	appendStringInfo(buf, "node: %u/%u/%u blkno: %u",
					 node.spcNode, node.dbNode, node.relNode, blkno);
}

void
vodka_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_VODKA_CREATE_INDEX:
			appendStringInfoString(buf, "Create index, ");
			desc_node(buf, *(RelFileNode *) rec, VODKA_ROOT_BLKNO);
			break;
		case XLOG_VODKA_CREATE_PTREE:
			appendStringInfoString(buf, "Create posting tree, ");
			desc_node(buf, ((vodkaxlogCreatePostingTree *) rec)->node, ((vodkaxlogCreatePostingTree *) rec)->blkno);
			break;
		case XLOG_VODKA_INSERT:
			{
				vodkaxlogInsert *xlrec = (vodkaxlogInsert *) rec;
				char	*payload = rec + sizeof(vodkaxlogInsert);

				appendStringInfoString(buf, "Insert item, ");
				desc_node(buf, xlrec->node, xlrec->blkno);
				appendStringInfo(buf, " isdata: %c isleaf: %c",
								 (xlrec->flags & VODKA_INSERT_ISDATA) ? 'T' : 'F',
								 (xlrec->flags & VODKA_INSERT_ISLEAF) ? 'T' : 'F');
				if (!(xlrec->flags & VODKA_INSERT_ISLEAF))
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
				if (!(xlrec->flags & VODKA_INSERT_ISDATA))
					appendStringInfo(buf, " isdelete: %c",
									 (((vodkaxlogInsertEntry *) payload)->isDelete) ? 'T' : 'F');
				else if (xlrec->flags & VODKA_INSERT_ISLEAF)
				{
					vodkaxlogRecompressDataLeaf *insertData =
						(vodkaxlogRecompressDataLeaf *) payload;

					appendStringInfo(buf, " unmodified: %u length: %u (compressed)",
									 insertData->unmodifiedsize,
									 insertData->length);
				}
				else
				{
					vodkaxlogInsertDataInternal *insertData = (vodkaxlogInsertDataInternal *) payload;
					appendStringInfo(buf, " pitem: %u-%u/%u",
									 PostingItemGetBlockNumber(&insertData->newitem),
									 ItemPointerGetBlockNumber(&insertData->newitem.key),
									 ItemPointerGetOffsetNumber(&insertData->newitem.key));
				}
			}
			break;
		case XLOG_VODKA_SPLIT:
			{
				vodkaxlogSplit *xlrec = (vodkaxlogSplit *) rec;

				appendStringInfoString(buf, "Page split, ");
				desc_node(buf, ((vodkaxlogSplit *) rec)->node, ((vodkaxlogSplit *) rec)->lblkno);
				appendStringInfo(buf, " isrootsplit: %c", (((vodkaxlogSplit *) rec)->flags & VODKA_SPLIT_ROOT) ? 'T' : 'F');
				appendStringInfo(buf, " isdata: %c isleaf: %c",
								 (xlrec->flags & VODKA_INSERT_ISDATA) ? 'T' : 'F',
								 (xlrec->flags & VODKA_INSERT_ISLEAF) ? 'T' : 'F');
			}
			break;
		case XLOG_VODKA_VACUUM_PAGE:
			appendStringInfoString(buf, "Vacuum page, ");
			desc_node(buf, ((vodkaxlogVacuumPage *) rec)->node, ((vodkaxlogVacuumPage *) rec)->blkno);
			break;
		case XLOG_VODKA_VACUUM_DATA_LEAF_PAGE:
			{
				vodkaxlogVacuumDataLeafPage *xlrec = (vodkaxlogVacuumDataLeafPage *) rec;
				appendStringInfoString(buf, "Vacuum data leaf page, ");
				desc_node(buf, xlrec->node, xlrec->blkno);
				appendStringInfo(buf, " unmodified: %u length: %u",
							 xlrec->data.unmodifiedsize,
							 xlrec->data.length);
			}
			break;
		case XLOG_VODKA_DELETE_PAGE:
			appendStringInfoString(buf, "Delete page, ");
			desc_node(buf, ((vodkaxlogDeletePage *) rec)->node, ((vodkaxlogDeletePage *) rec)->blkno);
			break;
		case XLOG_VODKA_UPDATE_META_PAGE:
			appendStringInfoString(buf, "Update metapage, ");
			desc_node(buf, ((vodkaxlogUpdateMeta *) rec)->node, VODKA_METAPAGE_BLKNO);
			break;
		case XLOG_VODKA_INSERT_LISTPAGE:
			appendStringInfoString(buf, "Insert new list page, ");
			desc_node(buf, ((vodkaxlogInsertListPage *) rec)->node, ((vodkaxlogInsertListPage *) rec)->blkno);
			break;
		case XLOG_VODKA_DELETE_LISTPAGE:
			appendStringInfo(buf, "Delete list pages (%d), ", ((vodkaxlogDeleteListPages *) rec)->ndeleted);
			desc_node(buf, ((vodkaxlogDeleteListPages *) rec)->node, VODKA_METAPAGE_BLKNO);
			break;
		default:
			appendStringInfo(buf, "unknown vodka op code %u", info);
			break;
	}
}
