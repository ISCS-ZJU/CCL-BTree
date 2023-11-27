#include <x86intrin.h>
#include <stdint.h>

#define CACHE_LINE_SIZE 64
#define USE_SFENCE
#define USE_CLWB

inline void fence()
{
#ifdef USE_SFENCE
    _mm_sfence();
#else
    _mm_mfence();
#endif
}

inline void clflush(void *addr, int len, bool is_log = false)
{

    for (uint64_t uptr = (uint64_t)addr & ~(CACHE_LINE_SIZE - 1); uptr < (uint64_t)addr + len; uptr += CACHE_LINE_SIZE)
    {

#ifndef eADR_TEST

#ifdef USE_CLWB
        _mm_clwb((void *)uptr);
#else
        _mm_clflushopt((void *)uptr);
        // asm volatile(".byte 0x66; clflush %0"
        //              : "+m"(*(volatile char *)uptr));
#endif
#endif
    }

    fence();
}

inline void clflush_nofence(void *addr, int len, bool is_log = false)
{

    for (uint64_t uptr = (uint64_t)addr & ~(CACHE_LINE_SIZE - 1); uptr < (uint64_t)addr + len; uptr += CACHE_LINE_SIZE)
    {
#ifndef eADR_TEST
        _mm_clwb((void *)uptr);
#endif
    }
}
