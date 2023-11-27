// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef _RLU_ARCH_H
#define _RLU_ARCH_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __KERNEL__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#ifndef likely
#define likely(x) __builtin_expect((unsigned long)(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((unsigned long)(x), 0)
#endif
#else /* __KERNEL__ */
#include <linux/printk.h>
#include <linux/string.h>
#include <linux/slab.h>
#include <linux/bug.h>
#endif /* __KERNEL__ */

#ifndef __read_mostly
#define __read_mostly __attribute__((__section__(".data..read_mostly")))
#endif

/*
 * machine-, architecture-specific information
 */
#define ____ptr_aligned __attribute__((aligned(sizeof(void *))))

#ifndef __KERNEL__
#define PAGE_SIZE 4096
#define L1_CACHE_BYTES 64
#define ____cacheline_aligned __attribute__((aligned(L1_CACHE_BYTES)))
#endif /* __KERNEL__ */

#define CACHE_LINE_PREFETCH_UNIT (2)
#define CACHE_DEFAULT_PADDING                                                  \
	((CACHE_LINE_PREFETCH_UNIT * L1_CACHE_BYTES) / sizeof(long))

#define ____cacheline_aligned2                                                 \
	__attribute__((aligned(CACHE_LINE_PREFETCH_UNIT * L1_CACHE_BYTES)))

#define ____page_aligned __attribute__((aligned(PAGE_SIZE)))

#ifndef __packed
#define __packed __attribute__((packed))
#endif

/*#ifndef static_assert
#define static_assert(e) (sizeof(struct { int : (-!(e)); }))
#define static_assert_msg(e, msg) static_assert(e)
#endif*/

#ifndef __KERNEL__
static inline void __attribute__((__always_inline__)) smp_mb(void)
{
	__asm__ __volatile__("mfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_rmb(void)
{
	__asm__ __volatile__("lfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_wmb(void)
{
	__asm__ __volatile__("sfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) barrier(void)
{
	__asm__ __volatile__("" ::: "memory");
}
#endif

static inline void __attribute__((__always_inline__)) smp_wmb_tso(void)
{
	barrier();
}

#define smp_cas(__ptr, __old_val, __new_val)                                   \
	__sync_bool_compare_and_swap(__ptr, __old_val, __new_val)

#define smp_cas_v(__ptr, __old_val, __new_val, __fetched_val)                  \
	({                                                                     \
		(__fetched_val) = __sync_val_compare_and_swap(                 \
			__ptr, __old_val, __new_val);                          \
		(__fetched_val) == (__old_val);                                \
	})

#define smp_cas16b(__ptr, __old_val1, __old_val2, __new_val1, __new_val2)      \
	({                                                                     \
		char result;                                                   \
		__asm__ __volatile__("lock; cmpxchg16b %0; setz %1"            \
				     : "=m"(*(__ptr)), "=a"(result)            \
				     : "m"(*(__ptr)), "d"(__old_val2),         \
				       "a"(__old_val1), "c"(__new_val2),       \
				       "b"(__new_val1)                         \
				     : "memory");                              \
		(int)result;                                                   \
	})

#define smp_swap(__ptr, __val) __sync_lock_test_and_set(__ptr, __val)

#define smp_atomic_load(__ptr)                                                 \
	({ __sync_val_compare_and_swap(__ptr, __ptr, __ptr); })

#define smp_atomic_store(__ptr, __val) (void)smp_swap(__ptr, __val)

#define smp_faa(__ptr, __val) __sync_fetch_and_add(__ptr, __val)

#define smp_fas(__ptr, __val) __sync_fetch_and_sub(__ptr, __val)

#define cpu_relax() asm volatile("pause\n" : : : "memory")

static inline uint64_t __attribute__((__always_inline__)) read_tsc(void)
{
	uint32_t a, d;
	__asm __volatile("rdtsc" : "=a"(a), "=d"(d));
	return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline unsigned long read_coreid_rdtscp(int *chip, int *core) {
    unsigned long a,d,c;
    __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
    *chip = (c & 0xFFF000)>>12;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << 32);;
}



static inline uint64_t __attribute__((__always_inline__)) read_tscp(void)
{
	uint32_t a, d;
	__asm __volatile("rdtscp" : "=a"(a), "=d"(d));
	return ((uint64_t)a) | (((uint64_t)d) << 32);
}

#ifndef __KERNEL__
static inline void cpuid(int i, unsigned int *a, unsigned int *b,
			 unsigned int *c, unsigned int *d)
{
	/* https://bit.ly/2uGziVO */
	__asm __volatile("cpuid"
			 : "=a"(*a), "=b"(*b), "=c"(*c), "=d"(*d)
			 : "a"(i), "c"(0));
}
#endif

static inline unsigned int max_cpu_freq(void)
{
	/* https://bit.ly/2EbkRZp */
	unsigned int regs[4];
	cpuid(0x16, &regs[0], &regs[1], &regs[2], &regs[3]);
	return regs[1];
}


#define cache_prefetchr_high(__ptr) __builtin_prefetch((void *)__ptr, 0, 3)

#define cache_prefetchr_mid(__ptr) __builtin_prefetch((void *)__ptr, 0, 2)

#define cache_prefetchr_low(__ptr) __builtin_prefetch((void *)__ptr, 0, 0)

#define cache_prefetchw_high(__ptr) __builtin_prefetch((void *)__ptr, 1, 3)

#define cache_prefetchw_mid(__ptr) __builtin_prefetch((void *)__ptr, 1, 2)

#define cache_prefetchw_low(__ptr) __builtin_prefetch((void *)__ptr, 1, 0)
#ifdef NO_CLWB
static inline void clwb(volatile void *p)
{
        asm volatile("clflush (%0)" ::"r"(p));
}
#else
static inline void clwb(volatile void *p)
{
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (p));
}
#endif

#ifdef __cplusplus
}
#endif

#endif /* _RLU_ARCH_H */
