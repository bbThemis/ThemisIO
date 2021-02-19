#ifndef __LIST_H__
#define __LIST_H__

typedef struct list_head_t
{
    struct list_head_t *next, *prev;
}list_head;

#define LIST_POISON1  ((void *)0x00100100)
#define LIST_POISON2  ((void *)0x00200200)
//#define INVALID_LIST (NULL)

inline void INIT_LIST_HEAD(list_head *list)
{
    list->next = list;
    list->prev = list;
}

inline void __list_add(list_head *newp, list_head *prev, list_head *next)
{
    next->prev = newp;
    newp->next = next;
    newp->prev = prev;
    prev->next = newp;
}

inline void list_add(list_head *newp, list_head *head)
{
    __list_add(newp, head, head->next);
}

inline void list_add_tail(list_head *newp, list_head *head)
{
    __list_add(newp, head->prev, head);
}

inline void __list_del(list_head *prev, list_head *next)
{
    next->prev = prev;
    prev->next = next;
}

inline void __list_del_entry(list_head *entry)
{
    __list_del(entry->prev, entry->next);
}

inline void list_del(list_head *entry)
{
    __list_del(entry->prev, entry->next);
	
    entry->next = (list_head *)LIST_POISON1;
    entry->prev = (list_head *)LIST_POISON2;
//    entry->next = (list_head *)INVALID_LIST;
//    entry->prev = (list_head *)INVALID_LIST;
}

inline void list_replace(list_head *old, list_head *newp)
{
    newp->next = old->next;
    newp->next->prev = newp;
    newp->prev = old->prev;
    newp->prev->next = newp;
}

inline int list_empty(const list_head *head)
{
    return head->next == head;
}

#define container_of(ptr, type, member) \
        ((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

#define list_entry(ptr, type, member) \
        container_of(ptr, type, member)

#define list_for_each(pos, head) \
        for (pos = (head)->next; pos != (head); pos = pos->next)

#endif
