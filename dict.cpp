#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <string.h>

#include "dict.h"
#include "xxhash.h"
#include "utility.h"

#define GROWTH_FACTOR (2)
#define MAX_LOAD_FACTOR (0.80)

//int first_av;
//struct elt elt_list[INITIAL_SIZE];
//struct elt *pelt_list[INITIAL_SIZE];

//struct dict *d=NULL;
//struct elt *elt_list=NULL;
//int *ht_table=NULL;

// Totoal size = sizeof(dict) + sizeof(struct elt *)*INITIAL_SIZE + sizeof(struct elt)*INITIAL_SIZE
// memory region three parts. 1) dict ( sizeof(dict) ) 2) pelt_list[] ( sizeof(struct elt *)*INITIAL_SIZE ) 3) elt_list ( sizeof(struct elt)*INITIAL_SIZE )

/** Jenkins' hash function for 64-bit integers. */
inline unsigned long long __ac_Jenkins_hash_64(unsigned long long key)
{
    key += ~(key << 32);
    key ^= (key >> 22);
    key += ~(key << 13);
    key ^= (key >> 8);
    key += (key << 3);
    key ^= (key >> 15);
    key += ~(key << 27);
    key ^= (key >> 31);
    return key;
}

/* Hash a key for a particular hash table. */
inline unsigned long long fast_hash(int size, unsigned long long key) 
{
	return key & size;
	//	unsigned long long hashval;
	//	hashval = __ac_Jenkins_hash_64(key);
	//	return hashval & size;	// assume d->size is a power of 2
	////	return hashval % d->size;
	/* return key % hashtable->num_buckets; */
	//  return key & (hashtable->num_buckets - 1);
}

void CHASHTABLE_INT::DictCreate(unsigned long int nSize, struct elt_Int ** p_elt_list, int ** p_ht_table)
{
	int i;
	int *p_mutex_attr;
	pthread_mutexattr_t mattr;
	
	if(nSize) {
		if(is_power_of_two(nSize)==0)	{
			printf("Error: hashtable nSize = %x. It is not a power of 2.\nQuit.\n", nSize);
			exit(1);
		}
		size = nSize - 1;
		n = 0;
		
		p_mutex_attr = (int *)(&mattr);
		*p_mutex_attr = PTHREAD_MUTEXATTR_FLAG_PSHARED;	// PTHREAD_PROCESS_SHARED !!!!!!!!!!!!!!! Shared between processes
		if(pthread_mutex_init(&lock, &mattr) != 0) {
			perror("pthread_mutex_init");
			exit(1);
		}
		Offset_ht_table = sizeof(CHASHTABLE_INT);
		Offset_elt_list = sizeof(CHASHTABLE_INT) + sizeof(int)*(size+1);
	}

	*p_ht_table = (int *)((char *)this + sizeof(CHASHTABLE_INT));
	*p_elt_list = (struct elt_Int *)((char *)this + sizeof(CHASHTABLE_INT) + sizeof(int)*(size+1));
	
	if(nSize)	{
		for(i = 0; i <= size; i++) (*p_ht_table)[i] = -1;
		first_av = 0;
		for(i=0; i<=size; i++)	{
			(*p_elt_list)[i].next = i+1;
			(*p_elt_list)[i].key = -1;
		}
		(*p_elt_list)[size].next = -1;	// the end
	}
}

// insert a new key-value pair into an existing dictionary 
int CHASHTABLE_INT::DictInsert(const int key, const int value, struct elt_Int ** p_elt_list, int ** p_ht_table)
{
    struct elt_Int *e;
    unsigned long long h;
	int first_av_Save;
	
	//assert(key);
	first_av_Save = first_av;
	e = &( (*p_elt_list)[first_av]);	// first available unit
	e->key = key;
    e->value = value;
	
	first_av = e->next;	// pointing to the next available unit
	
	//h = XXH64(key, nLen, 0) % d->size;
	h = fast_hash(size, key);
	
    e->next = (*p_ht_table)[h];
    (*p_ht_table)[h] = first_av_Save;
    n++;
	
    /* grow table if there is not enough room */
    if(n >= (size * MAX_LOAD_FACTOR) ) {
		printf("Hash table is FULL.\nQuit.\n");
		exit(1);
		//        grow(d);
    }

	return first_av_Save;	// the unit saving the data
}

int CHASHTABLE_INT::DictInsertAuto(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table)
{
    struct elt_Int *e;
    unsigned long long h;
	int first_av_Save;
	
	//    assert(key);
	
	first_av_Save = first_av;
	e = &( (*p_elt_list)[first_av]);	// first available unit
	e->key = key;
    e->value = first_av_Save;
	
	first_av = e->next;	// pointing to the next available unit
	
	//    h = XXH64(key, strlen(key), 0) % d->size;
	h = fast_hash(size, key);
	
    e->next = (*p_ht_table)[h];
    (*p_ht_table)[h] = first_av_Save;
    n++;
	
    /* grow table if there is not enough room */
    if(n >= (size * MAX_LOAD_FACTOR) ) {
		printf("Hash table is FULL.\nQuit.\n");
		exit(1);
		//        grow(d);
    }
	return first_av_Save;	// the unit saving the data
}
/* return the most recently inserted value associated with a key */
/* or 0 if no matching key is present */
int CHASHTABLE_INT::DictSearch(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash)
{
	int idx;
	struct elt_Int *e;
	
	if(n == 0) return (-1);
	
	*fn_hash = fast_hash(size, key);
	idx = (*p_ht_table)[*fn_hash];
	//	*fn_hash = XXH64(key, strlen(key), 0);
	//	idx = (*p_ht_table)[(*fn_hash)% d->size];
	if(idx == -1)	{
		return (-1);
	}
	
	e = &( (*p_elt_list)[idx] );
    while(1) {
		//        if(!strcmp(e->key, key)) {
        if(e->key == key) {
            return e->value;
        }
		else	{
			idx = e->next;
			if(idx == -1)	{	// end
				return (-1);
			}
			e = &( (*p_elt_list)[idx] );
		}
    }
	
    return -1;
}

int CHASHTABLE_INT::DictSearchOrg(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table)
{
	int idx;
	struct elt_Int *e;
	unsigned long long fn_hash;
	
	if(n == 0) return (-1);
	
	fn_hash = fast_hash(size, key);
	//	fn_hash = XXH64(key, strlen(key), 0);
	idx = (*p_ht_table)[fn_hash];
	//	idx = (*p_ht_table)[(fn_hash)% d->size];
	if(idx == -1)	{
		return (-1);
	}
	
	e = &( (*p_elt_list)[idx] );
    while(1) {
        if(e->key == key) {
			//        if(!strcmp(e->key, key)) {
            return e->value;
        }
		else	{
			idx = e->next;
			if(idx == -1)	{	// end
				return (-1);
			}
			e = &( (*p_elt_list)[idx] );
		}
    }
	
    return -1;
}

// delete the most recently inserted record with the given key 
// if there is no such record, has no effect 
void CHASHTABLE_INT::DictDelete(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table)
{
    int idx, next;
    unsigned long long h;
	
	h = fast_hash(size, key);
	//	h = XXH64(key, strlen(key), 0) % d->size;
	idx = (*p_ht_table)[h];
	
	//	if(!strcmp((*p_elt_list)[idx].key, key)) {	// found as the first element
	if((*p_elt_list)[idx].key == key) {	// found as the first element
		(*p_ht_table)[h] = (*p_elt_list)[idx].next;
		(*p_elt_list)[idx].next = first_av;
		first_av = idx;		// put back as the beginning of the free space
		n--;
		return;
	}
	
	next = (*p_elt_list)[idx].next;
    for(; next != -1; next = (*p_elt_list)[idx].next) {
		//        if(!strcmp((*p_elt_list)[next].key, key)) {
        if((*p_elt_list)[next].key == key ) {
			(*p_elt_list)[idx].next = (*p_elt_list)[next].next;
			(*p_elt_list)[next].next = first_av;
			first_av = next;		
			n--;
			return;
        }
        idx = next;
    }
}



void CHASHTABLE_CHAR::DictCreate(unsigned long int nSize, struct elt_Char ** p_elt_list, int ** p_ht_table)
{
	int i;
	int *p_mutex_attr;
	pthread_mutexattr_t mattr;
	
	if(nSize) {
		if(is_power_of_two(nSize)==0)	{
			printf("Error: hashtable nSize = %x. It is not a power of 2.\nQuit.\n", nSize);
			exit(1);
		}
		size = nSize - 1;
		n = 0;
		
		p_mutex_attr = (int *)(&mattr);
		*p_mutex_attr = PTHREAD_MUTEXATTR_FLAG_PSHARED;	// PTHREAD_PROCESS_SHARED !!!!!!!!!!!!!!! Shared between processes
		if(pthread_mutex_init(&lock, &mattr) != 0) {
			perror("pthread_mutex_init");
			exit(1);
		}
		Offset_ht_table = sizeof(CHASHTABLE_CHAR);
		Offset_elt_list = sizeof(CHASHTABLE_CHAR) + sizeof(int)*(size+1);
	}

	*p_ht_table = (int *)((char *)this + sizeof(CHASHTABLE_CHAR));
	*p_elt_list = (struct elt_Char *)((char *)this + sizeof(CHASHTABLE_CHAR) + sizeof(int)*(size+1));
	
	if(nSize)	{
		for(i = 0; i <= size; i++) (*p_ht_table)[i] = -1;
		first_av = 0;
		for(i=0; i<= size; i++)	{
			(*p_elt_list)[i].next = i+1;
		}
		(*p_elt_list)[size].next = -1;	// the end
	}
}

// insert a new key-value pair into an existing dictionary 
int CHASHTABLE_CHAR::DictInsert(const char *key, const int value, struct elt_Char ** p_elt_list, int ** p_ht_table)
{
    struct elt_Char *e;
    unsigned long long h;
	int first_av_Save;
	
	assert(key);
	first_av_Save = first_av;
	e = &( (*p_elt_list)[first_av]);	// first available unit
	strcpy(e->key, key);
    e->value = value;
	
	first_av = e->next;	// pointing to the next available unit
	
	h = XXH64(key, strlen(key), 0) & size;
//	h = fast_hash(size, key);
	
    e->next = (*p_ht_table)[h];
    (*p_ht_table)[h] = first_av_Save;
    n++;
	
    /* grow table if there is not enough room */
    if(n >= (size * MAX_LOAD_FACTOR) ) {
		printf("Hash table is FULL.\nQuit.\n");
		exit(1);
		//        grow(d);
    }

	return first_av_Save;	// the unit saving the data
}

int CHASHTABLE_CHAR::DictInsertAuto(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table)
{
    struct elt_Char *e;
    unsigned long long h;
	int first_av_Save;
	
	assert(key);
	
	first_av_Save = first_av;
	e = &( (*p_elt_list)[first_av]);	// first available unit
	strcpy(e->key, key);
//	e->key = key;
    e->value = first_av_Save;
	
	first_av = e->next;	// pointing to the next available unit
	
	h = XXH64(key, strlen(key), 0) & size;
//	h = fast_hash(size, key);
	
    e->next = (*p_ht_table)[h];
    (*p_ht_table)[h] = first_av_Save;
    n++;
	
    /* grow table if there is not enough room */
//    if(n >= (size * MAX_LOAD_FACTOR) ) {
//		printf("Hash table is FULL.\nQuit.\n");
//		exit(1);
//		//        grow(d);
//    }
	return first_av_Save;	// the unit saving the data
}
/* return the most recently inserted value associated with a key */
/* or 0 if no matching key is present */
int CHASHTABLE_CHAR::DictSearch(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash)
{
	int idx;
	struct elt_Char *e;
	
	if(n == 0) return (-1);
	assert(key);
	
//	*fn_hash = fast_hash(size, key);
//	idx = (*p_ht_table)[*fn_hash];
	*fn_hash = XXH64(key, strlen(key), 0);
	idx = (*p_ht_table)[(*fn_hash) & size];
	if(idx == -1)	{
		return (-1);
	}
	
	e = &( (*p_elt_list)[idx] );
    while(1) {
		if(!strcmp(e->key, key)) {
            return e->value;
        }
		else	{
			idx = e->next;
			if(idx == -1)	{	// end
				return (-1);
			}
			e = &( (*p_elt_list)[idx] );
		}
    }
	
    return -1;
}

int CHASHTABLE_CHAR::DictSearchOrg(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table)
{
	int idx;
	struct elt_Char *e;
	unsigned long long fn_hash;
	
	if(n == 0) return (-1);
	assert(key);
	
//	fn_hash = fast_hash(size, key);
	fn_hash = XXH64(key, strlen(key), 0);
//	idx = (*p_ht_table)[fn_hash];
	idx = (*p_ht_table)[(fn_hash) & size];
	if(idx == -1)	{
		return (-1);
	}
	
	e = &( (*p_elt_list)[idx] );
    while(1) {
		if(!strcmp(e->key, key)) {
            return e->value;
        }
		else	{
			idx = e->next;
			if(idx == -1)	{	// end
				return (-1);
			}
			e = &( (*p_elt_list)[idx] );
		}
    }
	
    return -1;
}

// delete the most recently inserted record with the given key 
// if there is no such record, has no effect 
void CHASHTABLE_CHAR::DictDelete(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table)
{
    int idx, next;
    unsigned long long h;
	
	assert(key);
//	h = fast_hash(size, key);
	h = XXH64(key, strlen(key), 0) & size;
	idx = (*p_ht_table)[h];
	
	if(!strcmp((*p_elt_list)[idx].key, key)) {	// found as the first element
		(*p_ht_table)[h] = (*p_elt_list)[idx].next;
		(*p_elt_list)[idx].next = first_av;
		first_av = idx;		// put back as the beginning of the free space
		n--;
		return;
	}
	
	next = (*p_elt_list)[idx].next;
    for(; next != -1; next = (*p_elt_list)[idx].next) {
		if(!strcmp((*p_elt_list)[next].key, key)) {
			(*p_elt_list)[idx].next = (*p_elt_list)[next].next;
			(*p_elt_list)[next].next = first_av;
			first_av = next;		
			n--;
			return;
        }
        idx = next;
    }
}
