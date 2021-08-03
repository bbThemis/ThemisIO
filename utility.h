#ifndef __UTILITY_H__
#define __UTILITY_H__

#include <stdint.h>

// min and max conflict with std::min and std::max
//#ifndef min(a,b)
#define MIN(a,b)	( ((a)<(b)) ? (a) : (b) )
#define MAX(a,b)	( ((a)>(b)) ? (a) : (b) )
//#endif

#ifndef	BUDDY_PAGE_SHIFT
#define BUDDY_PAGE_SHIFT	(12)	// assume page size is 4096
#endif

static inline uint64_t rotl(const uint64_t x, int k) {
	return (x << k) | (x >> (64 - k));
}

static inline uint64_t next(uint64_t s[2])
{
	const uint64_t s0 = s[0];
	uint64_t s1 = s[1];
	const uint64_t result = s0 + s1;

	s1 ^= s0;
	s[0] = rotl(s0, 24) ^ s1 ^ (s1 << 16); // a, b
	s[1] = rotl(s1, 37); // c

	return result;
}

static inline unsigned long int Cal_Order(unsigned long int x)
{
        unsigned long int order=0, pow2=1;

		x = (x & 0xFFF) ? ( (x >> BUDDY_PAGE_SHIFT) + 1) : (x >> BUDDY_PAGE_SHIFT);	// x/4096
        while(pow2<x)   {
                pow2 = pow2 << 1;
                order++;
        }
        return order;
}

//static unsigned long int pow2roundup (unsigned long int x);
//static int is_power_of_two(unsigned long int x);

static inline unsigned long int pow2roundup (unsigned long int x)
{
    if (x==0) return 1;
    --x;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return x+1;
}

static inline int is_power_of_two(unsigned long int x)
{
        return ((x != 0) && !(x & (x - 1)));
}

static void Get_BaseName(char szHostName[])
{
	int i=0;
	while(szHostName[i] != 0)	{
		if(szHostName[i] == '.')	{
			szHostName[i] = 0;	// truncate hostname[]
			return;
		}
		i++;
	}
}

/*
 *  * Atomic compare and exchange.  Compare OLD with MEM, if identical,
 *   * store NEW in MEM.  Return the initial value in MEM.  Success is
 *    * indicated by comparing RETURN with OLD.
 *     */

/*
static inline unsigned long __cmpxchg(volatile unsigned long *ptr, unsigned long old,
                                      unsigned long new_value, int size)
{
        unsigned long prev;
        switch (size) {
        case 1:
                __asm__ __volatile__("lock ; cmpxchgb %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
        case 2:
                __asm__ __volatile__("lock ; cmpxchgw %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
        case 4:
                __asm__ __volatile__("lock ; cmpxchg %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
         case 8:
                __asm__ __volatile__("lock ; cmpxchgq %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
       }
        return old;
}
*/

static inline size_t __cmpxchg(volatile size_t *ptr, size_t old, size_t new_value, int size)
{
        int prev;
        switch (size) {
        case 1:
                __asm__ __volatile__("lock ; cmpxchgb %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
        case 2:
                __asm__ __volatile__("lock ; cmpxchgw %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
        case 4:
                __asm__ __volatile__("lock ; cmpxchg %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
         case 8:
                __asm__ __volatile__("lock ; cmpxchgq %2,%1"
                                     : "=a"(prev),"+m"(*ptr)
                                     : "q"(new_value), "0"(old)
                                     : "memory");
                return prev;
       }
        return old;
}

static inline int fetch_and_add(int* variable, int value)
{
    __asm__ volatile("lock; xaddl %0, %1"
      : "+r" (value), "+m" (*variable) // input + output
      : // No input-only
      : "memory"
    );
    return value;
}

inline void clflush(volatile void *p)
{
        asm volatile ("clflush (%0)" :: "r"(p));
}

inline void Atomic_Increase(size_t new_value, size_t *pAddr)	// compare new_value with *pAddr, update *pAddr = new_value if (new_value > *pAddr). 
{
	size_t value;
	size_t old_value = *pAddr;
	while (old_value < new_value) {
		value = __cmpxchg(pAddr, old_value, new_value, 8);
		if (value != old_value) {
			old_value = value;
			continue;
		}
	}
}

#endif


