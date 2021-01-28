#ifndef __HASHTABLE_H__
#define __HASHTABLE_H__

#include <pthread.h>
#include <stdio.h>
#include <string.h>

#define PTHREAD_MUTEXATTR_FLAG_PSHARED (0x80000000)	// int 
#define INITIAL_SIZE (1024*1024*2)
#define MAX_NAME_LEN	(184)

struct elt_Int {
    int next;
    int value;
	int key;
	int pad;
};	// 16 bytes

struct elt_Char {
    int next;	// 
    int value;
    char key[MAX_NAME_LEN];
};	// 192 bytes

class CHASHTABLE_INT {
public:
    int size;           // size of the pointer table
    int n;              // number of elements stored
	int first_av;
	int pad;
	pthread_mutex_t lock;	// 40 bytes
	int Offset_ht_table;	// Only save the offset since the memory address in different processes could be different!!!
	int Offset_elt_list;

	static int GetStorageSize(int nSize)	{	return ( sizeof(CHASHTABLE_INT) + sizeof(int)*nSize*2 + sizeof(struct elt_Int)*nSize*2 );	}
	void DictCreate(unsigned long int nSize, struct elt_Int ** p_elt_list, int ** p_ht_table);
	int DictInsertAuto(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table);
	int DictInsert(const int key, const int value, struct elt_Int ** p_elt_list, int ** p_ht_table);
	int DictSearch(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash);
	int DictSearchOrg(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table);
	void DictDelete(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table);
};

class CHASHTABLE_CHAR {
public:
    int size;           // size of the pointer table
    int n;              // number of elements stored
	int first_av;
	int pad;
	pthread_mutex_t lock;	// 40 bytes
	int Offset_ht_table;	// Only save the offset since the memory address in different processes could be different!!!
	int Offset_elt_list;

	static int GetStorageSize(int nSize)	{	return ( sizeof(CHASHTABLE_CHAR) + sizeof(int)*nSize*2 + sizeof(struct elt_Char)*nSize*2 );	}
	void DictCreate(unsigned long int nSize, struct elt_Char ** p_elt_list, int ** p_ht_table);
	int DictInsertAuto(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table);
	int DictInsert(const char *key, const int value, struct elt_Char ** p_elt_list, int ** p_ht_table);
	int DictSearch(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash);
	int DictSearchOrg(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table);
	void DictDelete(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table);
};

#endif

