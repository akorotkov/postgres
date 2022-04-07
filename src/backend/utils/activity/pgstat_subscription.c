/* -------------------------------------------------------------------------
 *
 * pgstat_subscription.c
 *	  Implementation of subscription statistics.
 *
 * This file contains the implementation of subscription statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_subscription.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"


/*
 * Report a subscription error.
 */
void
pgstat_report_subscription_error(Oid subid, bool is_apply_error)
{
	PgStat_MsgSubscriptionError msg;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_SUBSCRIPTIONERROR);
	msg.m_subid = subid;
	msg.m_is_apply_error = is_apply_error;
	pgstat_send(&msg, sizeof(PgStat_MsgSubscriptionError));
}

/*
 * Report dropping the subscription.
 */
void
pgstat_report_subscription_drop(Oid subid)
{
	PgStat_MsgSubscriptionDrop msg;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_SUBSCRIPTIONDROP);
	msg.m_subid = subid;
	pgstat_send(&msg, sizeof(PgStat_MsgSubscriptionDrop));
}
