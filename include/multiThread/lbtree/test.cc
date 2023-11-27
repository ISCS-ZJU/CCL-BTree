/* Filename:has_tsx.c */
#include <cpuid.h>
#include <stddef.h>
#include <stdio.h>

#define CPUID_RTM (1 << 11)
#define CPUID_HLE (1 << 4)

//gcc -o test test.cc
static inline int cpu_has_rtm(void)
{
    if (__get_cpuid_max(0, NULL) >= 7) {
        unsigned a, b, c, d;
        __cpuid_count(7, 0, a, b, c, d);
        return !!(b & CPUID_RTM);
    }
    return 0;
}

static inline int cpu_has_hle(void)
{
    if (__get_cpuid_max(0, NULL) >= 7) {
        unsigned a, b, c, d;
        __cpuid_count(7, 0, a, b, c, d);
        return !!(b & CPUID_HLE);
    }
    return 0;
}

int main(void) {
    printf("RTM:%s", cpu_has_rtm()?"YES":"NO");
    printf("HLE:%s", cpu_has_hle()?"YES":"NO");
    return 0;
}