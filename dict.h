#ifndef __HASHTABLE_H__
#define __HASHTABLE_H__

#include <pthread.h>
#include <stdio.h>
#include <string.h>

#define PTHREAD_MUTEXATTR_FLAG_PSHARED (0x80000000)	// int 
#define INITIAL_SIZE (1024*1024*2)
#define MAX_NAME_LEN	(184)
//#define MAX_ENTRY_NAME_LEN	(56)
#define MAX_ENTRY_NAME_LEN    (40)

struct elt_Int {
    int next;
    int value;
	int key;
	int pad;
};	// 16 bytes

struct elt_MEMREG {
    int next;
    int value;
	long int key;
};	// 16 bytes

struct elt_Char {
    int next;	// 
    int value;
    char key[MAX_NAME_LEN];
};	// 192 bytes

struct elt_CharEntry {
    int next;	// 
    int value;	// store entry property here! If necessary, we can save offset to a buffer for very long name in future. 
    char key[MAX_ENTRY_NAME_LEN];	// This contains entry name list already! No need to store it elsewhere. 
};	// 192 bytes

class CHASHTABLE_INT {
public:
    int size;           // size of the pointer table
    int n;              // number of elements stored
	int first_av;
	int pad;
	pthread_mutex_t lock;	// 40 bytes

	static int GetStorageSize(int nSize)	{	return ( sizeof(CHASHTABLE_INT) + sizeof(int)*nSize*2 + sizeof(struct elt_Int)*nSize*2 );	}
	void DictCreate(unsigned long int nSize, struct elt_Int ** p_elt_list, int ** p_ht_table);
	int DictInsertAuto(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table);
	int DictInsert(const int key, const int value, struct elt_Int ** p_elt_list, int ** p_ht_table);
	int DictSearch(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash);
	int DictSearchOrg(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table);
	void DictDelete(const int key, struct elt_Int ** p_elt_list, int ** p_ht_table);
};

class CHASHTABLE_MEMREG {
public:
    int size;           // size of the pointer table
    int n;              // number of elements stored
	int first_av;
	int nBytesMemReg;	// the size of registered memory
	pthread_mutex_t lock;	// 40 bytes

	static int GetStorageSize(int nSize)	{	return ( sizeof(CHASHTABLE_MEMREG) + sizeof(int)*nSize*2 + sizeof(struct elt_MEMREG)*nSize*2 );	}
	void DictCreate(unsigned long int nSize, struct elt_MEMREG ** p_elt_list, int ** p_ht_table);
	int DictInsert(const long int key, const int value, struct elt_MEMREG ** p_elt_list, int ** p_ht_table);
	int DictSearch(const long int key, struct elt_MEMREG ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash);
	int DictSearchOrg(const long int key, struct elt_MEMREG ** p_elt_list, int ** p_ht_table);
	void DictDelete(const long int key, struct elt_MEMREG ** p_elt_list, int ** p_ht_table);
};

class CHASHTABLE_CHAR {
public:
    int size;           // size of the pointer table
    int n;              // number of elements stored
	int first_av;
	int pad;
	pthread_mutex_t lock;	// 40 bytes
//	int Offset_ht_table;	// Only save the offset since the memory address in different processes could be different!!!
//	int Offset_elt_list;

	static int GetStorageSize(int nSize)	{	return ( sizeof(CHASHTABLE_CHAR) + sizeof(int)*nSize*2 + sizeof(struct elt_Char)*nSize*2 );	}
	void DictCreate(unsigned long int nSize, struct elt_Char ** p_elt_list, int ** p_ht_table);
	int DictInsertAuto(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table);
	int DictInsert(const char *key, const int value, struct elt_Char ** p_elt_list, int ** p_ht_table);
	int DictSearch(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash);
	int DictSearchOrg(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table);
	void DictDelete(const char *key, struct elt_Char ** p_elt_list, int ** p_ht_table);
};

class CHASHTABLE_DirEntry {
public:
    int size;           // size of the pointer table
    int n;              // number of elements stored
	int first_av;
	int pad;
//	pthread_mutex_t lock;	// 40 bytes

	static int GetStorageSize(int nSize)	{	return ( sizeof(CHASHTABLE_DirEntry) + sizeof(int)*nSize*2 + sizeof(struct elt_CharEntry)*nSize*2 );	}
	void DictCreate(unsigned long int nSize, struct elt_CharEntry ** p_elt_list, int ** p_ht_table);
	int DictInsertAuto(const char *key, struct elt_CharEntry ** p_elt_list, int ** p_ht_table);
	int DictInsert(const char *key, const int value, struct elt_CharEntry ** p_elt_list, int ** p_ht_table);
	int DictSearch(const char *key, struct elt_CharEntry ** p_elt_list, int ** p_ht_table, unsigned long long *fn_hash);
	int DictSearchOrg(const char *key, struct elt_CharEntry ** p_elt_list, int ** p_ht_table);
	int DictDelete(const char *key, struct elt_CharEntry ** p_elt_list, int ** p_ht_table);
};

#endif

