#ifndef __UTILITY_H__
#define __UTILITY_H__


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


#endif


