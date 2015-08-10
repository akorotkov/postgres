/*-------------------------------------------------------------------------
 *
 * barrier.h
 *	  Memory barrier operations.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/barrier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BARRIER_H
#define BARRIER_H

#include "storage/s_lock.h"
#include "port/atomics.h"

extern slock_t dummy_spinlock;

#endif   /* BARRIER_H */
