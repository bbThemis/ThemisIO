#ifndef __BUDDY_H__
#define __BUDDY_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "list.h"

#define PTHREAD_MUTEXATTR_FLAG_PSHARED (0x80000000)	// int 

enum pageflags{
    PG_head,    //����buddyϵͳ�ڣ��׸�ҳ
	PG_tail,    //����buddyϵͳ�ڣ���ҳ֮���ҳ��
	PG_buddy   //��buddyϵͳ��
};

#define BUDDY_PAGE_SHIFT    (12UL)
#define BUDDY_PAGE_SIZE     (1UL << BUDDY_PAGE_SHIFT)	// 4096 bytes per page
#define BUDDY_MAX_ORDER     (26UL)


struct page
{
    // spin_lock        lock;
    list_head			lru;
    unsigned long       flags;
    union {
        unsigned long   order;
        struct page     *first_page;
    };
};

struct free_area
{
    list_head			free_list;
    unsigned long       nr_free;
};

struct mem_zone
{
    unsigned long       page_num;
    unsigned long       page_size;
    struct page        *first_page;
    unsigned long       start_addr;
    unsigned long       end_addr;
    struct free_area    free_area[BUDDY_MAX_ORDER];
};

struct mem_block {
    struct mem_zone     zone;
    struct page        *pages;
};


class CMEM_ALLOCATOR {
public:
	//	struct mem_block global_mem_block;
    struct mem_zone		zone;
    struct page			*pages;
	unsigned long int	nPages;
	//	list_head			page_list;
	pthread_mutex_t lock;	// 40 bytes
	
	int Mem_Allocator_Init(unsigned long int nPages, void *pMem_Pages, void *pMem_Data);
	void Mem_Allocator_Destroy(void)	{	
		free((void*)pages);
		free((void*)zone.start_addr);
	}
	void * Mem_Alloc(unsigned long size, size_t *nBytesAllocated);
	void Mem_Free(void *);
	void Mem_Batch_Free(void **);

	unsigned long Get_Num_Free_Page(void);

	struct page* Virt_to_Page(void *ptr);
	void * Page_to_Virt(struct page *page);
	
private:
	void buddy_system_init(struct page *start_page, unsigned long start_addr);
//	struct page* Get_Pages(unsigned long order);
	void         Free_Pages(struct page *page);
};


/*
* ҳ��Ϊ���ࣺһ���ǵ�ҳ��zero page��,
* һ�������ҳ��compound page����
* ���ҳ�ĵ�һ����head������Ϊtail��
* */
inline void __SetPageHead(struct page *page)
{
    page->flags |= (1UL<<PG_head);
}

inline void __SetPageTail(struct page *page)
{
    page->flags |= (1UL<<PG_tail);
}

inline void __SetPageBuddy(struct page *page)
{
    page->flags |= (1UL<<PG_buddy);
}
/**/
inline void __ClearPageHead(struct page *page)
{
    page->flags &= ~(1UL<<PG_head);
}

inline void __ClearPageTail(struct page *page)
{
    page->flags &= ~(1UL<<PG_tail);
}

inline void __ClearPageBuddy(struct page *page)
{
    page->flags &= ~(1UL<<PG_buddy);
}

inline int PageHead(struct page *page)
{
    return (page->flags & (1UL<<PG_head));
}

inline int PageTail(struct page *page)
{
    return (page->flags & (1UL<<PG_tail));
}

inline int PageBuddy(struct page *page)
{
    return (page->flags & (1UL<<PG_buddy));
}

/*
* ����ҳ��order��PG_buddy��־
* */
inline void set_page_order_buddy(struct page *page, unsigned long order)
{
    page->order = order;
    __SetPageBuddy(page);
}

inline void rmv_page_order_buddy(struct page *page)
{
    page->order = 0;
    __ClearPageBuddy(page);
}

inline unsigned long
__find_buddy_index(unsigned long page_idx, unsigned int order)
{
    return (page_idx ^ (1 << order));
}

inline unsigned long
__find_combined_index(unsigned long page_idx, unsigned int order)
{
    return (page_idx & ~(1 << order));
}

/*
* Linux�ں˽����ҳ��order��¼�ڵڶ���ҳ���prevָ����
* ��ϵͳ�����ҳ��order��¼���׸�ҳ���page->order����
* */
inline unsigned long compound_order(struct page *page)
{
    if (!PageHead(page))
        return 0; //��ҳ
    //return (unsigned long)page[1].lru.prev;
    return page->order;
}

inline void set_compound_order(struct page *page, unsigned long order)
{
    //page[1].lru.prev = (void *)order;
    page->order = order;
}

inline void BUDDY_BUG(const char *f, int line)
{
    printf("BUDDY_BUG in %s, %d.\n", f, line);
    assert(0);
}

// print buddy system status
void dump_print(struct mem_zone *zone);
void dump_print_dot(struct mem_zone *zone);

#endif
