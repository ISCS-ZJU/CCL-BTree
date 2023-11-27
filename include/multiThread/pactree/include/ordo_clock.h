// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef _ORDO_CLOCK_H
#define _ORDO_CLOCK_H

#ifdef __cplusplus
extern "C" {
#endif

#include "arch.h"

#define ORDO_LESS_THAN (-1)
#define ORDO_GREATER_THAN (1)
#define ORDO_UNCERTAIN (0)

/* ORDO boundary of our evaluation set up
 * 224-core machine: 1214 clock cycles
 * 120-core machine: 650 clock cycles */
//#define __ORDO_BOUNDARY (1214)
#define __ORDO_BOUNDARY (0)

#ifdef ORDO_CONFIGURABLE_BOUNDARY
/* Since clock difference is a read-mostly variable that is never
 * updated once it is initialized. Thus, to completely prevent
 * the false sharing of its cacheline, we put padding around it. */
static unsigned long _g_ordo_array[2 * CACHE_DEFAULT_PADDING] __read_mostly;
#define g_ordo_boundary (_g_ordo_array[CACHE_DEFAULT_PADDING])
#else
#define g_ordo_boundary __ORDO_BOUNDARY
#endif /* ORDO_CONFIGURABLE_BOUNDARY */

static inline void ordo_clock_init(void)
{
#ifdef ORDO_CONFIGURABLE_BOUNDARY
	g_ordo_boundary = __ORDO_BOUNDARY;
#endif
}

static inline unsigned long ordo_boundary(void)
{
	return g_ordo_boundary;
}

static inline unsigned long ordo_get_clock(void)
{
	/* rdtscp() is a serializing variant, which is not
	 * reordered in an instruction pipeline. */
	return read_tscp();
}

static inline unsigned long ordo_get_clock_relaxed(void)
{
	/* rdtsc() is not a serializing instruction so
	 * rdtsc could be reordered in an instruction pipeline.
	 * If rdtsc() can be barrier-ed with other instructions,
	 * such as memory fences, it is okay to use rdtsc()
	 * without worrying such reordering. */
	return read_tsc();
}

static inline int ordo_lt_clock(unsigned long t1, unsigned long t2)
{
	return (t1 + g_ordo_boundary) < t2;
}

static inline int ordo_gt_clock(unsigned long t1, unsigned long t2)
{
	return t1 > (t2 + g_ordo_boundary);
}

static inline int ordo_cmp_clock(unsigned long t1, unsigned long t2)
{
	if (ordo_lt_clock(t1, t2))
		return ORDO_LESS_THAN;
	if (ordo_gt_clock(t1, t2))
		return ORDO_GREATER_THAN;
	return ORDO_UNCERTAIN;
}

static inline unsigned long ordo_new_clock(unsigned long t)
{
	unsigned long new_clk;

	while (!ordo_gt_clock(new_clk = ordo_get_clock(), t)) {
		smp_mb();
	}
	return new_clk;
}

#ifdef __cplusplus
}
#endif

#endif /* _ORDO_CLOCK_H */
