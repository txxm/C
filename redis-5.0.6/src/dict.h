/* 哈希表实现：
 * 此文件使用（insert/del/replace/find）实现内存中的哈希表获取随机元素的操作，
 * 哈希表根据需要自动调整大小到2^n，冲突由链式哈希解决。
 */

#include <stdint.h>

#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

/* 哈希表每个桶中节点结构 */
typedef struct dictEntry {
    void *key;                  /* 哈希表中的key */
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;                        /* 哈希表中的value（指针、整数、浮点数） */
    struct dictEntry *next;     /* 指向下一个节点 */
} dictEntry;

typedef struct dictType {
    uint64_t (*hashFunction)(const void *key);          /* 计算哈希值 */
    void *(*keyDup)(void *privdata, const void *key);   /* 复制key */
    void *(*valDup)(void *privdata, const void *obj);   /* 复制value */
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);  /* 比较key是否相等 */
    void (*keyDestructor)(void *privdata, void *key);   /* 删除key */
    void (*valDestructor)(void *privdata, void *obj);   /* 删除value */
} dictType;

/* 哈希表结构 */
typedef struct dictht {
    dictEntry **table;          /* 存放数组地址，数组用来存放桶 */
    unsigned long size;         /* 哈希表的大小 */
    unsigned long sizemask;     /* 哈希掩码(size-1) */
    unsigned long used;         /* 哈希表中有效节点 */
} dictht;

/* 字典结构 */
typedef struct dict {
    dictType *type;             /* 字典类型 */
    void *privdata;             /* 私有数据 */
    dictht ht[2];               /* 两个哈希表 */
    long rehashidx;             /* 重新哈希的计数，rehashidx=-1时表示没有进行rehash */
    unsigned long iterators;    /* 正在迭代的迭代器数量 */
} dict;

/* 字典迭代器 */
typedef struct dictIterator {
    dict *d;                        /* 迭代器对应的字典 */
    long index;                     /* 哈希表下标（桶索引） */
    int table, safe;                /* table--h[0]还是h[1]，safe--是否安全(1安全，可进行添加、查找等；不安全只能用dictNext()) */
    dictEntry *entry, *nextEntry;   /* entry--哈希表中的每一个桶，nextEntry--下一个桶 */
    long long fingerprint;          /* 不安全的迭代器，使用指纹来检测 */
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);
typedef void (dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- Macros ------------------------------------*/
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

#define dictHashKey(d, key) (d)->type->hashFunction(key)
#define dictGetKey(he) ((he)->key)                          /* 获取哈希表节点的key */
#define dictGetVal(he) ((he)->v.val)                        /* 获取哈希表节点的value */
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
#define dictIsRehashing(d) ((d)->rehashidx != -1)

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
dictEntry *dictAddOrFind(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
int dictDelete(dict *d, const void *key);
dictEntry *dictUnlink(dict *ht, const void *key);
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
void dictRelease(dict *d);
dictEntry * dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
void dictGetStats(char *buf, size_t bufsize, dict *d);
uint64_t dictGenHashFunction(const void *key, int len);
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
uint64_t dictGetHash(dict *d, const void *key);
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */
