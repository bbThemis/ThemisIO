#include <malloc.h>
#include "buddy.h"
#include "utility.h"

int CMEM_ALLOCATOR::Mem_Allocator_Init(unsigned long int npage, void *pMem_Pages, void *pMem_Data)
{
    long int pages_size;
    unsigned long start_addr;
	int *p_mutex_attr;
	pthread_mutexattr_t mattr;
	
	if(pMem_Pages)	{
		nPages = npage;
		pages = (struct page*)pMem_Pages;
		start_addr = (unsigned long)pMem_Data;
	}
	else	{
		nPages = npage;
		pages_size = nPages * sizeof(struct page);
		pages = (struct page*)memalign(4096, pages_size);
		if (!pages)	return -1;
		start_addr = (unsigned long)memalign(4096, nPages * BUDDY_PAGE_SIZE);
		if (!start_addr)	{
			free(pages);
			return -2;
		}
	}
    // init buddy
    buddy_system_init(pages, start_addr);
	//	INIT_LIST_HEAD(&page_list);
	
	p_mutex_attr = (int *)(&mattr);
	*p_mutex_attr = PTHREAD_MUTEXATTR_FLAG_PSHARED;	// PTHREAD_PROCESS_SHARED !!!!!!!!!!!!!!! Shared between processes
	if(pthread_mutex_init(&lock, &mattr) != 0) {
        perror("pthread_mutex_init");
        exit(1);
	}
	return 0;
}

void CMEM_ALLOCATOR::buddy_system_init(struct page *start_page, unsigned long start_addr)
{
    unsigned long i;
    struct page *page = NULL;
    struct free_area *area = NULL;
	
    zone.page_num = nPages;
    zone.page_size = BUDDY_PAGE_SIZE;
    zone.first_page = start_page;
    zone.start_addr = start_addr;
    zone.end_addr = start_addr + nPages * BUDDY_PAGE_SIZE;
	printf("DBG> start_addr = %lx  end_addr = %lx\n", zone.start_addr, zone.end_addr);
	
    // init each area
    for (i = 0; i < BUDDY_MAX_ORDER; i++)
    {
        area = zone.free_area + i;
        INIT_LIST_HEAD(&area->free_list);
        area->nr_free = 0;
    }
    memset(start_page, 0, nPages * sizeof(struct page));
    // init and free each page
    for (i = 0; i < nPages; i++)
    {
        page = zone.first_page + i;
        INIT_LIST_HEAD(&page->lru);
        // TODO: init page->lock
        Free_Pages(page);
    }
}

/* 设置组合页的属性 */
static void prepare_compound_pages(struct page *page, unsigned long order)
{
    unsigned long i;
    unsigned long nr_pages = (1UL<<order);
	
    // 首个page记录组合页的order值
    set_compound_order(page, order);
    __SetPageHead(page); // 首页设置head标志
    for(i = 1; i < nr_pages; i++)
    {
        struct page *p = page + i;
        __SetPageTail(p); // 其余页设置tail标志
        p->first_page = page;
    }
}

/* 将组合页进行分裂，以获得所需大小的页 */
static void expand(struct mem_zone *zone, struct page *page,
                   unsigned long low_order, unsigned long high_order,
                   struct free_area *area)
{
    unsigned long size = (1U << high_order);
    while (high_order > low_order)
    {
        area--;
        high_order--;
        size >>= 1;
        list_add(&page[size].lru, &area->free_list);
        area->nr_free++;
        // set page order
        set_page_order_buddy(&page[size], high_order);
    }
}

static struct page *__alloc_page(unsigned long order,
                                 struct mem_zone *zone)
{
    struct page *page = NULL;
    struct free_area *area = NULL;
    unsigned long current_order = 0;
	
    for (current_order = order;
	current_order < BUDDY_MAX_ORDER; current_order++)
    {
        area = zone->free_area + current_order;
        if (list_empty(&area->free_list)) {
            continue;
        }
        // remove closest size page
        page = list_entry(area->free_list.next, struct page, lru);
        list_del(&page->lru);
        rmv_page_order_buddy(page);
        area->nr_free--;
        // expand to lower order
        expand(zone, page, order, current_order, area);
        // compound page
        if (order > 0)
            prepare_compound_pages(page, order);
        else // single page
            page->order = 0;
        return page;
    }
    return NULL;
}

void * CMEM_ALLOCATOR::Mem_Alloc(unsigned long size, size_t *nBytesAllocated)
{
    struct page *page = NULL;
//    unsigned long order, nPageAV;
    unsigned long order;
    void *ret;
	
//	nPageAV = Get_Num_Free_Page();
	order = Cal_Order(size);
    if (order >= BUDDY_MAX_ORDER)
    {
        BUDDY_BUG(__FILE__, __LINE__);
        return NULL;
    }
    //TODO: lock zone->lock
	pthread_mutex_lock(&lock);

    page = __alloc_page(order, &zone);
//	list_add(&page->lru, &page_list);
    //TODO: unlock zone->lock

	ret = Page_to_Virt(page);
	*nBytesAllocated = page ? (BUDDY_PAGE_SIZE * (1 << order)) : (0L);
	pthread_mutex_unlock(&lock);

//	*nBytesAllocated = page ? (BUDDY_PAGE_SIZE * (1 << order)) : (0L);

//	printf("DBG> Allocated %ld. (%ld, %ld)", *nBytesAllocated, nPageAV, Get_Num_Free_Page());

//    return Page_to_Virt(page);
	return ret;
}

////////////////////////////////////////////////

/* 销毁组合页 */
static int destroy_compound_pages(struct page *page, unsigned long order)
{
    int bad = 0;
    unsigned long i;
    unsigned long nr_pages = (1UL<<order);
	
    __ClearPageHead(page);
    for(i = 1; i < nr_pages; i++)
    {
        struct page *p = page + i;
        if( !PageTail(p) || p->first_page != page )
        {
            bad++;
            BUDDY_BUG(__FILE__, __LINE__);
        }
        __ClearPageTail(p);
    }
    return bad;
}

#define PageCompound(page) (page->flags & ((1UL<<PG_head)|(1UL<<PG_tail)))
#define page_is_buddy(page,order) (PageBuddy(page) && (page->order == order))

void CMEM_ALLOCATOR::Free_Pages(struct page *page)
{
    unsigned long order = compound_order(page);
    unsigned long buddy_idx = 0, combinded_idx = 0;
    unsigned long page_idx = page - zone.first_page;
	
    //TODO: lock zone->lock
    if (PageCompound(page))	{
        if (destroy_compound_pages(page, order))	BUDDY_BUG(__FILE__, __LINE__);
    }
		
	while (order < BUDDY_MAX_ORDER-1)	{
			struct page *buddy;
			// find and delete buddy to combine
			buddy_idx = __find_buddy_index(page_idx, order);
			buddy = page + (buddy_idx - page_idx);
			if (!page_is_buddy(buddy, order))
				break;
			list_del(&buddy->lru);
			zone.free_area[order].nr_free--;
			// remove buddy's flag and order
			rmv_page_order_buddy(buddy);
			// update page and page_idx after combined
			combinded_idx = __find_combined_index(page_idx, order);
			page = page + (combinded_idx - page_idx);
			page_idx = combinded_idx;
			order++;
	}
	set_page_order_buddy(page, order);
	list_add(&page->lru, &(zone.free_area[order].free_list));
	zone.free_area[order].nr_free++;
	//TODO: unlock zone->lock
}
//void CMEM_ALLOCATOR::Free_Pages(struct page *page)
void CMEM_ALLOCATOR::Mem_Free(void *addr)
{
	pthread_mutex_lock(&lock);
	Free_Pages(Virt_to_Page(addr));
	pthread_mutex_unlock(&lock);
}

void CMEM_ALLOCATOR::Mem_Batch_Free(void **addr_list)
{
	int i=0;

	pthread_mutex_lock(&lock);
	while(addr_list[i])	{
		Free_Pages(Virt_to_Page(addr_list[i]));
//		printf("DBG> %ld free pages.\n", Get_Num_Free_Page());
		i++;
	}
	pthread_mutex_unlock(&lock);
}

////////////////////////////////////////////////

void * CMEM_ALLOCATOR::Page_to_Virt(struct page *page)
{
    unsigned long page_idx = 0;
    unsigned long address = 0;
	
    page_idx = page - zone.first_page;
    address = zone.start_addr + page_idx * BUDDY_PAGE_SIZE;
	
    return (void *)address;
}

struct page* CMEM_ALLOCATOR::Virt_to_Page(void *ptr)
{
    unsigned long page_idx = 0;
    struct page *page = NULL;
    unsigned long address = (unsigned long)ptr;
	
    if((address<zone.start_addr)||(address>zone.end_addr))
    {
        printf("start_addr=0x%lx, end_addr=0x%lx, address=0x%lx\n",
			zone.start_addr, zone.end_addr, address);
        BUDDY_BUG(__FILE__, __LINE__);
        return NULL;
    }
    page_idx = (address - zone.start_addr)>>BUDDY_PAGE_SHIFT;
	
    page = zone.first_page + page_idx;
    return page;
}

unsigned long CMEM_ALLOCATOR::Get_Num_Free_Page()
{
    unsigned long i, ret;
    for (i = 0, ret = 0; i < BUDDY_MAX_ORDER; i++)
    {
        ret += zone.free_area[i].nr_free * (1UL<<i);
    }
    return ret;
}

void dump_print(struct mem_zone *zone)
{
    unsigned long i;
    printf("order(npage) nr_free\n");
    for (i = 0; i < BUDDY_MAX_ORDER; i++)
    {
        printf("  %ld(%ld)   %ld\n", i, 1UL<<i, zone->free_area[i].nr_free);
    }
}

#define PRINT_PAGES_INFO    1

void dump_print_dot(struct mem_zone *zone)
{
    unsigned long i, j, k;
	
    FILE *fout;
    fout = fopen("bdgraph.dot", "w");
    assert(fout);
	
    // graph header
    fprintf(fout, "digraph g {\n");
    fprintf(fout, "graph [rankdir=LR];\n");
    fprintf(fout, "edge [dir=both,arrowsize=.5];\n");
    fprintf(fout, "node [shape=record,height=.1];\n");
    fprintf(fout, "\n");
	
    // free_area
    fprintf(fout, "free_area [label = \"");
    for (i = 0; i < BUDDY_MAX_ORDER; i++)
    {
        fprintf(fout, "<%ld>2^%ld-%ld", i, i,
			zone->free_area[i].nr_free);
        if (i+1 != BUDDY_MAX_ORDER)
            fprintf(fout, "|");
    }
    fprintf(fout, "\"];\n\n");
#if PRINT_PAGES_INFO
    fprintf(fout, "pages [style=filled,color=gray,label = \"{");
    for (i = zone->page_num - 1, k = 0; i >=0 ; i--)
    {
        if (PageBuddy(&zone->first_page[i]))
        {
            if (k == 0) k = 1;
            else fprintf(fout, "|");
            fprintf(fout, "<%ld>%ld~%ld", i,
				i+(1UL<<zone->first_page[i].order), i);
        }
    }
    fprintf(fout, "}\"];\n\n");
#endif
    // each list in free area
    for (i = 0; i < BUDDY_MAX_ORDER; i++)
    {
        list_head *pos;
        struct page *page;
        unsigned long page_idx;
        fprintf(fout, "// area %ld\n", i);
        j = 0;
        // each node in list
        list_for_each(pos, &zone->free_area[i].free_list)
        {
            page = list_entry(pos, struct page, lru);
            page_idx = page - zone->first_page;
            fprintf(fout, "node%ld_%ld [label = \"{%ld}\"];",
				i, j, page_idx);
#if PRINT_PAGES_INFO
            fprintf(fout, "node%ld_%ld -> pages:%ld;\n", i, j, page_idx);
#else
            fprintf(fout, "\n");
#endif
            j++;
        }
        // connect nodes
        for (k = 0; k < j; k++)
        {
            if (k == 0)
                fprintf(fout, "free_area:%ld -> node%ld_%ld;\n", i, i, k);
            else
                fprintf(fout, "node%ld_%ld -> node%ld_%ld;\n", i, k-1, i, k);
        }
        fprintf(fout, "\n");
    }
	
    // graph end
    fprintf(fout, "\n");
    fprintf(fout, "}\n");
    fclose(fout);
}

