// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include <cstring>
#include <iostream>
#include "Oplog.h"
#include "listNode.h"
#include "threadData.h"
#include "pactree.h"

thread_local int logCnt = 0;
thread_local int coreId = -1;
extern thread_local ThreadData *curThreadData;
std::mutex mtx[112];

ListNode ::ListNode()
{
    deleted = false;
    bitMap.clear();
    lastScanVersion = 0;
#ifdef STRINGKEY
    min = Key_t();
    max = Key_t();
    memset(max.data, 0xff, KEYLENGTH);
#endif
}

void ListNode::setCur(pptr<ListNode> ptr)
{
    this->curPtr = ptr;
}

void ListNode::setNext(pptr<ListNode> ptr)
{
    this->nextPtr = ptr;
}

void ListNode::setPrev(pptr<ListNode> ptr)
{
    this->prevPtr = ptr;
}

void ListNode::setMin(Key_t key)
{
    this->min = key;
}

void ListNode::setMax(Key_t key)
{
    this->max = key;
}

void ListNode::setFullBitmap()
{
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        bitMap.set(i);
    }
}

void ListNode::setDeleted(bool deleted)
{
    this->deleted = deleted;
}

ListNode *ListNode::getNext()
{
    ListNode *next = nextPtr.getVaddr();
    return next;
}

pptr<ListNode> ListNode::getNextPtr()
{
    return nextPtr;
}

pptr<ListNode> ListNode::getCurPtr()
{
    return curPtr;
}

ListNode *ListNode::getPrev()
{
    ListNode *prev = prevPtr.getVaddr();
    return prev;
}

pptr<ListNode> ListNode::getPrevPtr()
{
    return prevPtr;
}

version_t ListNode::readLock(uint64_t genId)
{
    return verLock.read_lock(genId);
}

version_t ListNode::writeLock(uint64_t genId)
{
    return verLock.write_lock(genId);
}

bool ListNode::readUnlock(version_t oldVersion)
{
    return verLock.read_unlock(oldVersion);
}

void ListNode::writeUnlock()
{
    return verLock.write_unlock();
}

int ListNode ::getNumEntries()
{
    int numEntries = 0;
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (bitMap[i])
        {
            numEntries++;
        }
    }
    return numEntries;
}

std::pair<Key_t, Val_t> *ListNode ::getKeyArray()
{
    return this->keyArray;
}

std::pair<Key_t, Val_t> *ListNode ::getValueArray()
{
    return this->keyArray;
}

Key_t ListNode ::getMin()
{
    return this->min;
}

Key_t ListNode::getMax()
{
    return this->max;
}

bool ListNode::getDeleted()
{
    return deleted;
}

bool compare2(std::pair<Key_t, uint8_t> &x, std::pair<Key_t, uint8_t> &y)
{
    return x.first < y.first;
}
bool compare(std::pair<Key_t, Val_t> &i, std::pair<Key_t, Val_t> &j)
{
    return i.first < j.first;
}

pptr<ListNode> ListNode ::split(Key_t key, Val_t val, uint8_t keyHash, int threadId, void **olog)
{
    // Find median
    std::pair<Key_t, Val_t> copyArray[MAX_ENTRIES];
    std::copy(std::begin(keyArray), std::end(keyArray), std::begin(copyArray));
    std::sort(std::begin(copyArray), std::end(copyArray), compare);
    Key_t median = copyArray[MAX_ENTRIES / 2].first;
    int newNumEntries = 0;
    Key_t newMin = median;

    int chip, core;
    read_coreid_rdtscp(&chip, &core);
    if (core != coreId)
        coreId = core;

    mtx[core].lock();
    int numLogsPerThread = 1000;
    int logIdx = numLogsPerThread * (core) + logCnt;
    logCnt++;
    if (logCnt == numLogsPerThread)
    {
        logCnt = 0;
    }
    OpStruct *oplog;

#ifdef MULTIPOOL
    uint16_t poolId = (uint16_t)(3 * chip + 1);
#else
    uint16_t poolId = 1;
#endif
    /*pptr<ListNode> oplogPtr;
    PMEMoid oid;
    PMem::alloc(poolId,sizeof(OpStruct),(void **)&(oplogPtr), &oid);
    oplog=(OpStruct *) oplogPtr.getVaddr();*/

    oplog = (OpStruct *)PMem::getOpLog(logIdx);
    // 1) Add Oplog and set the infomration for current(overflown) node.
    Oplog::writeOpLog(oplog, OpStruct::insert, newMin, (void *)curPtr.getRawPtr(), poolId, key, val);
    flushToNVM((char *)oplog, sizeof(OpStruct));
    smp_wmb();
    // 2) Allocate new data node and store persistent pointer to the oplog.
    pptr<ListNode> newNodePtr;

#ifdef PACTREE_ALLOC_TIME
    time_start = NowNanos(); // add by zhenxin
#endif

    PMem::alloc(poolId, sizeof(ListNode), (void **)&(newNodePtr), &(oplog->newNodeOid));

#ifdef PACTREE_ALLOC_TIME
    time_pm_allocation += ElapsedNanos(time_start); // add by zhenxin
    // printf("max alloc time = %lld\n", time_pm_allocation);
#endif
    if (newNodePtr.getVaddr() == nullptr)
    {
        exit(1);
    }
    unsigned long nPtr = newNodePtr.getRawPtr();

    ListNode *newNode = (ListNode *)new (newNodePtr.getVaddr()) ListNode();

    // 3) Perform Split Operation
    //    3-1) Update New node
    int removeIndex = -1;
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (keyArray[i].first >= median)
        {
            newNode->insertAtIndex(keyArray[i], newNumEntries, fingerPrint[i], false);
            newNumEntries++;
        }
    }
    if (key >= newMin)
        newNode->insertAtIndex(std::make_pair(key, val), newNumEntries, keyHash, false);
    newNode->setMin(newMin);
    newNode->setNext(nextPtr);
    newNode->setCur(newNodePtr);
    newNode->setDeleted(false);

    newNode->setPrev(curPtr);
    newNode->setMax(getMax());
    setMax(newMin);
    flushToNVM((char *)newNode, sizeof(ListNode));
    smp_wmb();

    //    3-2) Setting next pointer to the New node
    nextPtr = newNodePtr;

    //    3-3) Update overflown node
    //    3-4) Make copy of the bitmap and update the bitmap value.
    hydra::bitset curBitMap = *(this->getBitMap());
    int numEntries = getNumEntries();
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (keyArray[i].first >= median)
        {
            curBitMap.reset(i);
            numEntries--;
            if (removeIndex == -1)
                removeIndex = i;
        }
    }

    //    3-4) copy updated bitmap to overflown node.
    //         What if the size of node is bigger than 64 entries?
    bitMap = curBitMap;
    flushToNVM((char *)this, L1_CACHE_BYTES);
    smp_wmb();
    if (key < newMin)
    {
        if (removeIndex != -1)
            insertAtIndex(std::make_pair(key, val), removeIndex, keyHash, true);
        else
            insertAtIndex(std::make_pair(key, val), numEntries, keyHash, true);
    }

    ListNode *nextNode = newNodePtr->getNext();
    nextNode->setPrev(newNodePtr);
    Oplog::enqPerThreadLog(oplog);
    *olog = oplog;
    mtx[core].unlock();

    return newNodePtr;
}

pptr<ListNode> ListNode ::split(Key_t key, Val_t val, uint8_t keyHash, int threadId)
{
    // Find median
    std::pair<Key_t, Val_t> copyArray[MAX_ENTRIES];
    std::copy(std::begin(keyArray), std::end(keyArray), std::begin(copyArray));
    std::sort(std::begin(copyArray), std::end(copyArray), compare);
    Key_t median = copyArray[MAX_ENTRIES / 2].first;
    int newNumEntries = 0;
    Key_t newMin = median;

    int chip, core;
    read_coreid_rdtscp(&chip, &core);
    if (core != coreId)
        coreId = core;

    mtx[core].lock();
    int numLogsPerThread = 1000;
    int logIdx = numLogsPerThread * (core) + logCnt;
    logCnt++;
    if (logCnt == numLogsPerThread)
    {
        logCnt = 0;
    }
    OpStruct *oplog;

#ifdef MULTIPOOL
    uint16_t poolId = (uint16_t)(3 * chip + 1);
#else
    uint16_t poolId = 1;
#endif
    /*    pptr<ListNode> oplogPtr;
        PMEMoid oid;
        PMem::alloc(poolId,sizeof(OpStruct),(void **)&(oplogPtr), &oid);
        oplog=(OpStruct *) oplogPtr.getVaddr();*/
    //    oplog = (OpStruct *)PMem::getOpLog(logIdx);
    oplog = new OpStruct();
    // 1) Add Oplog and set the infomration for current(overflown) node.
    Oplog::writeOpLog(oplog, OpStruct::insert, newMin, (void *)curPtr.getRawPtr(), poolId, key, val);
    flushToNVM((char *)oplog, sizeof(OpStruct));
    // smp_wmb();
    // exit(1); //case 1

    // 2) Allocate new data node and store persistent pointer to the oplog.
    pptr<ListNode> newNodePtr;

#ifdef PACTREE_ALLOC_TIME
    time_start = NowNanos(); // add by zhenxin
#endif

    PMem::alloc(poolId, sizeof(ListNode), (void **)&(newNodePtr), &(oplog->newNodeOid));
    //    PMEMoid asd;
    //    PMem::alloc(poolId,sizeof(ListNode),(void **)&(newNodePtr),&(asd)); //debug

#ifdef PACTREE_ALLOC_TIME
    time_pm_allocation += ElapsedNanos(time_start); // add by zhenxin
    // printf("max alloc time = %lld\n", time_pm_allocation);
#endif

    if (newNodePtr.getVaddr() == nullptr)
    {
        exit(1);
    }
    unsigned long nPtr = newNodePtr.getRawPtr();

    ListNode *newNode = (ListNode *)new (newNodePtr.getVaddr()) ListNode();

    // 3) Perform Split Operation
    //    3-1) Update New node
    int removeIndex = -1;
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (keyArray[i].first >= median)
        {
            newNode->insertAtIndex(keyArray[i], newNumEntries, fingerPrint[i], false);
            newNumEntries++;
        }
    }
    if (key >= newMin)
        newNode->insertAtIndex(std::make_pair(key, val), newNumEntries, keyHash, false);
    newNode->setMin(newMin);
    newNode->setNext(nextPtr);
    newNode->setCur(newNodePtr);
    newNode->setDeleted(false);

    newNode->setPrev(curPtr);
    newNode->setMax(getMax());
    setMax(newMin);
    flushToNVM((char *)newNode, sizeof(ListNode));
    smp_wmb();

    //    3-2) Setting next pointer to the New node
    nextPtr = newNodePtr;
    // exit(1); //case 2

    //    3-3) Update overflown node
    //    3-4) Make copy of the bitmap and update the bitmap value.
    hydra::bitset curBitMap = *(this->getBitMap());
    int numEntries = getNumEntries();
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (keyArray[i].first >= median)
        {
            curBitMap.reset(i);
            numEntries--;
            if (removeIndex == -1)
                removeIndex = i;
        }
    }

    //    3-4) copy updated bitmap to overflown node.
    //         What if the size of node is bigger than 64 entries?
    bitMap = curBitMap;
    flushToNVM((char *)this, L1_CACHE_BYTES);
    smp_wmb();
    if (key < newMin)
    {
        if (removeIndex != -1)
            insertAtIndex(std::make_pair(key, val), removeIndex, keyHash, true);
        else
            insertAtIndex(std::make_pair(key, val), numEntries, keyHash, true);
    }

    ListNode *nextNode = newNodePtr->getNext();
    nextNode->setPrev(newNodePtr);
    Oplog::enqPerThreadLog(oplog);

    mtx[core].unlock();
    // exit(1); //case 3
    return newNodePtr;
}

ListNode *ListNode ::mergeWithNext(uint64_t genId)
{

    // exit(1);
    ListNode *deleteNode = nextPtr.getVaddr();
    if (!deleteNode->writeLock(genId))
        return nullptr;

    // 1) Add Oplog and set the infomration for current(overflown) node.

    int numLogsPerThread = 1000;

    int chip, core;
    read_coreid_rdtscp(&chip, &core);
    if (core != coreId)
        coreId = core;

    mtx[core].lock();
    int logIdx = numLogsPerThread * (core) + logCnt;
    logCnt++;
    OpStruct *oplog = (OpStruct *)PMem::getOpLog(logIdx);

#ifdef MULTIPOOL
    uint16_t poolId = (uint16_t)(3 * chip + 1);
#else
    uint16_t poolId = 1;
#endif

    Oplog::writeOpLog(oplog, OpStruct::remove, getMin(), (void *)getNextPtr().getRawPtr(), poolId, -1, -1);
    oplog->newNodeOid = pmemobj_oid(deleteNode);
    flushToNVM((char *)oplog, sizeof(OpStruct));
    smp_wmb();
    //    printf("case 4\n");
    //   exit(0); //case 4

    int cur = 0;
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (deleteNode->getBitMap()->test(i))
        {
            for (int j = cur; j < MAX_ENTRIES; j++)
            {
                if (!bitMap[j])
                {
                    uint8_t keyHash = deleteNode->getFingerPrintArray()[i];
                    std::pair<Key_t, Val_t> &kv = deleteNode->getKeyArray()[i];
                    insertAtIndex(kv, j, keyHash, false);
                    cur = j + 1;
                    break;
                }
            }
        }
    }
    flushToNVM((char *)this, sizeof(ListNode));
    smp_wmb();
    ListNode *next = deleteNode->getNext();
    setMax(deleteNode->getMax());
    deleteNode->setDeleted(true);
    this->setNext(deleteNode->getNextPtr());
    flushToNVM((char *)this, L1_CACHE_BYTES);
    smp_wmb();
    next->setPrev(curPtr);
    flushToNVM((char *)next, L1_CACHE_BYTES);
    smp_wmb();
    mtx[core].unlock();
    // printf("case 5\n");
    // exit(0); //case 4

    return deleteNode;
}

ListNode *ListNode ::mergeWithPrev(uint64_t genId)
{
    ListNode *deleteNode = this;
    ListNode *mergeNode = prevPtr.getVaddr();
    if (!mergeNode->writeLock(genId))
        return nullptr;

    // 1) Add Oplog and set the infomration for current(overflown) node.

    int numLogsPerThread = 1000;

    int chip, core;
    read_coreid_rdtscp(&chip, &core);
    if (core != coreId)
        coreId = core;

    mtx[core].lock();
    int logIdx = numLogsPerThread * (core) + logCnt;
    logCnt++;
    OpStruct *oplog = (OpStruct *)PMem::getOpLog(logIdx);

#ifdef MULTIPOOL
    uint16_t poolId = (uint16_t)(3 * chip + 1);
#else
    uint16_t poolId = 1;
#endif

    Oplog::writeOpLog(oplog, OpStruct::remove, mergeNode->getMin(), (void *)mergeNode->getNextPtr().getRawPtr(), poolId, -1, -1);
    oplog->newNodeOid = pmemobj_oid(deleteNode);
    flushToNVM((char *)oplog, sizeof(OpStruct));
    smp_wmb();
    // printf("case 6\n");
    // exit(1); //case 6

    int cur = 0;
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (deleteNode->getBitMap()->test(i))
        {
            for (int j = cur; j < MAX_ENTRIES; j++)
            {
                if (!mergeNode->getBitMap()->test(j))
                {
                    uint8_t keyHash = deleteNode->getFingerPrintArray()[i];
                    Key_t key = deleteNode->getKeyArray()[i].first;
                    Val_t val = deleteNode->getValueArray()[i].second;
                    mergeNode->insertAtIndex(std::make_pair(key, val), j, keyHash, false);
                    cur = j + 1;
                    break;
                }
            }
        }
    }
    flushToNVM((char *)mergeNode, sizeof(ListNode));
    smp_wmb();
    // printf("case 7\n");
    // exit(1); //case 7

    ListNode *next = deleteNode->getNext();
    mergeNode->setMax(deleteNode->getMax());
    mergeNode->setNext(deleteNode->getNextPtr());
    flushToNVM((char *)mergeNode, L1_CACHE_BYTES);
    smp_wmb();
    // printf("case 8\n");
    // exit(1); //case 8

    next->setPrev(prevPtr);
    flushToNVM((char *)next, L1_CACHE_BYTES);
    smp_wmb();

    // printf("case 9\n");
    // exit(1); //case 9
    mergeNode->writeUnlock();
    mtx[core].unlock();
    return deleteNode;
}

bool ListNode ::insertAtIndex(std::pair<Key_t, Val_t> key, int index, uint8_t keyHash, bool flush)
{
    keyArray[index] = key;
    if (flush)
    {
        flushToNVM((char *)&keyArray[index], sizeof(keyArray[index]));
    }
    // printf("Debug: after first flush\n");
    fingerPrint[index] = keyHash;
    if (flush)
    {
        flushToNVM((char *)&fingerPrint[index], sizeof(uint8_t));
        smp_wmb();
    }
    // printf("Debug: after second flush\n");
    bitMap.set(index);
    if (flush)
    {
        flushToNVM((char *)this, L1_CACHE_BYTES);
        smp_wmb();
    }
    // printf("Debug: after third flush\n");
    return true;
}

bool ListNode ::updateAtIndex(Val_t value, int index)
{
    keyArray[index].second = value;
    flushToNVM((char *)&keyArray[index], sizeof(keyArray[index]));
    smp_wmb();
    return true;
}

bool ListNode ::removeFromIndex(int index)
{
    bitMap.reset(index);
    flushToNVM((char *)this, L1_CACHE_BYTES);
    smp_wmb();
    return true;
}

uint8_t ListNode ::getKeyInsertIndex(Key_t key)
{
    for (uint8_t i = 0; i < MAX_ENTRIES; i++)
    {
        if (!bitMap[i])
            return i;
    }
}

#if MAX_ENTRIES == 64

int ListNode::getKeyIndex(Key_t key, uint8_t keyHash)
{
    __m512i v1 = _mm512_loadu_si512((__m512i *)fingerPrint);
    __m512i v2 = _mm512_set1_epi8((int8_t)keyHash);
    uint64_t mask = _mm512_cmpeq_epi8_mask(v1, v2);
    uint64_t bitMapMask = bitMap.to_ulong();
    uint64_t posToCheck_64 = (mask)&bitMapMask;
    uint32_t posToCheck = (uint32_t)posToCheck_64;
    while (posToCheck)
    {
        int pos;
        asm("bsrl %1, %0"
            : "=r"(pos)
            : "r"((uint32_t)posToCheck));
        if (keyArray[pos].first == key)
            return pos;
        posToCheck = posToCheck & (~(1 << pos));
    }
    posToCheck = (uint32_t)(posToCheck_64 >> 32);
    while (posToCheck)
    {
        int pos;
        asm("bsrl %1, %0"
            : "=r"(pos)
            : "r"((uint32_t)posToCheck));
        if (keyArray[pos + 32].first == key)
            return pos + 32;
        posToCheck = posToCheck & (~(1 << pos));
    }
    return -1;
}

int ListNode::getFreeIndex(Key_t key, uint8_t keyHash)
{
    int freeIndex;
    int numEntries = getNumEntries();
    if (numEntries != 0 && getKeyIndex(key, keyHash) != -1)
        return -1;

    uint64_t bitMapMask = bitMap.to_ulong();
    if (!(~bitMapMask))
        return -2;

    uint64_t freeIndexMask = ~(bitMapMask);
    if ((uint32_t)freeIndexMask)
        asm("bsf %1, %0"
            : "=r"(freeIndex)
            : "r"((uint32_t)freeIndexMask));
    else
    {
        freeIndexMask = freeIndexMask >> 32;
        asm("bsf %1, %0"
            : "=r"(freeIndex)
            : "r"((uint32_t)freeIndexMask));
        freeIndex += 32;
    }
    return freeIndex;
}
#elif MAX_ENTRIES == 128

int ListNode::getKeyIndex(Key_t key, uint8_t keyHash)
{
    __m512i v1 = _mm512_loadu_si512((__m512i *)fingerPrint);
    __m512i v2 = _mm512_set1_epi8((int8_t)keyHash);
    uint64_t mask = _mm512_cmpeq_epi8_mask(v1, v2);
    uint64_t bitMapMask = bitMap.to_ulong(0);
    uint64_t posToCheck_64 = (mask)&bitMapMask;
    uint32_t posToCheck = (uint32_t)posToCheck_64;
    while (posToCheck)
    {
        int pos;
        asm("bsrl %1, %0"
            : "=r"(pos)
            : "r"((uint32_t)posToCheck));
        if (keyArray[pos].first == key)
            return pos;
        posToCheck = posToCheck & (~(1 << pos));
    }
    posToCheck = (uint32_t)(posToCheck_64 >> 32);
    while (posToCheck)
    {
        int pos;
        asm("bsrl %1, %0"
            : "=r"(pos)
            : "r"((uint32_t)posToCheck));
        if (keyArray[pos + 32].first == key)
            return pos + 32;
        posToCheck = posToCheck & (~(1 << pos));
    }
    v1 = _mm512_loadu_si512((__m512i *)&fingerPrint[64]);
    mask = _mm512_cmpeq_epi8_mask(v1, v2);
    bitMapMask = bitMap.to_ulong(1);
    posToCheck_64 = mask & bitMapMask;
    posToCheck = (uint32_t)posToCheck_64;
    while (posToCheck)
    {
        int pos;
        asm("bsrl %1, %0"
            : "=r"(pos)
            : "r"((uint32_t)posToCheck));
        if (keyArray[pos + 64].first == key)
            return pos + 64;
        posToCheck = posToCheck & (~(1 << pos));
    }
    posToCheck = (uint32_t)(posToCheck_64 >> 32);
    while (posToCheck)
    {
        int pos;
        asm("bsrl %1, %0"
            : "=r"(pos)
            : "r"((uint32_t)posToCheck));
        if (keyArray[pos + 96].first == key)
            return pos + 96;
        posToCheck = posToCheck & (~(1 << pos));
    }
    return -1;
}

int ListNode::getFreeIndex(Key_t key, uint8_t keyHash)
{
    int freeIndex;
    int numEntries = getNumEntries();
    if (numEntries != 0 && getKeyIndex(key, keyHash) != -1)
        return -1;

    uint64_t bitMapMask_lower = bitMap.to_ulong(0);
    uint64_t bitMapMask_upper = bitMap.to_ulong(1);
    if ((~(bitMapMask_lower) == 0x0) && (~(bitMapMask_upper) == 0x0))
        return -2;
    else if (~(bitMapMask_lower) != 0x0)
    {

        uint64_t freeIndexMask = ~(bitMapMask_lower);
        if ((uint32_t)freeIndexMask)
            asm("bsf %1, %0"
                : "=r"(freeIndex)
                : "r"((uint32_t)freeIndexMask));
        else
        {
            freeIndexMask = freeIndexMask >> 32;
            asm("bsf %1, %0"
                : "=r"(freeIndex)
                : "r"((uint32_t)freeIndexMask));
            freeIndex += 32;
        }

        return freeIndex;
    }
    else
    {
        uint64_t freeIndexMask = ~(bitMapMask_upper);
        if ((uint32_t)freeIndexMask)
        {
            asm("bsf %1, %0"
                : "=r"(freeIndex)
                : "r"((uint32_t)freeIndexMask));
            freeIndex += 64;
        }
        else
        {
            freeIndexMask = freeIndexMask >> 32;
            asm("bsf %1, %0"
                : "=r"(freeIndex)
                : "r"((uint32_t)freeIndexMask));
            freeIndex += 96;
        }

        return freeIndex;
    }
}

#else
int ListNode::getKeyIndex(Key_t key, uint8_t keyHash)
{
    int count = 0;
    int numEntries = getNumEntries();
    for (uint8_t i = 0; i < MAX_ENTRIES; i++)
    {
        if (bitMap[i] && keyHash == fingerPrint[i] && keyArray[i].first == key)
            return (int)i;
        if (bitMap[i])
            count++;
        if (count == numEntries)
            break;
    }
    return -1;
}

int ListNode::getFreeIndex(Key_t key, uint8_t keyHash)
{
    int freeIndex = -2;
    int count = 0;
    int numEntries = getNumEntries();
    bool keyCheckNeeded = true;
    for (uint8_t i = 0; i < MAX_ENTRIES; i++)
    {
        if (keyCheckNeeded && bitMap[i] && keyHash == fingerPrint[i] && keyArray[i].first == key)
            return -1;
        if (freeIndex == -2 && !bitMap[i])
            freeIndex = i;
        if (bitMap[i])
        {
            count++;
            if (count == numEntries)
                keyCheckNeeded = false;
        }
    }
    return freeIndex;
}
#endif

bool ListNode::insert(Key_t key, Val_t value, int threadId)
{
    uint8_t keyHash = getKeyFingerPrint(key);
    int index = getFreeIndex(key, keyHash);
    // printf("Debug: got FP and Free Index\n");
    if (index == -1)
        return false; // Key exitst
    if (index == -2)
    { // No free index
        // printf("Debug: before split\n");
        pptr<ListNode> newNodePtr = split(key, value, keyHash, threadId);
        ListNode *newNode = newNodePtr.getVaddr();
        ListNode *nextNode = newNode->getNext();
        nextNode->setPrev(newNodePtr);
        // printf("Debug: after split\n");
        return true;
    }
    // printf("Debug: before insertAtIndex\n");
    if (!insertAtIndex(std::make_pair(key, value), (uint8_t)index, keyHash, true))
    {
        // printf("Debug: after insertAtIndex\n");
        return false;
    }
    return true;
}
#ifdef SYNC
int ListNode::insert(Key_t key, Val_t value, void **locked_parent_node)
{
    uint8_t keyHash = getKeyFingerPrint(key);
    int index = getFreeIndex(key, keyHash);
    if (index == -1)
        return false; // Key exitst
    if (index == -2)
    { // No free index
        SearchLayer *sl = g_perNumaSlPtr[0];
        OpStruct *oplog;
        pptr<ListNode> newNodePtr = split(key, value, keyHash, 0, (void **)&oplog);
        ListNode *newNode = newNodePtr.getVaddr();
        ListNode *nextNode = newNode->getNext();
        nextNode->setPrev(newNodePtr);
        if (*locked_parent_node != nullptr)
        {
            sl->nodeUnlock(*locked_parent_node);
            *locked_parent_node = nullptr;
        }

        void *nNode = reinterpret_cast<void *>(((unsigned long)(oplog->poolId)) << 48 | (oplog->newNodeOid.off));
        sl->insert(oplog->key, (void *)nNode);
        writeUnlock();
        return true;
    }
    if (!insertAtIndex(std::make_pair(key, value), (uint8_t)index, keyHash, true))
    {
        writeUnlock();
        return false;
    }
    writeUnlock();
    return true;
}
#endif

bool ListNode::update(Key_t key, Val_t value)
{
    uint8_t keyHash = getKeyFingerPrint(key);
    int index = getKeyIndex(key, keyHash);
    if (index == -1)
        return false; // Key Does not exit
    if (!updateAtIndex(value, (uint8_t)index))
        return false;
    return true;
}

bool ListNode ::remove(Key_t key, uint64_t genId)
{
    uint8_t keyHash = getKeyFingerPrint(key);
    int index = getKeyIndex(key, keyHash);
    int numEntries = getNumEntries();
    ListNode *prev = prevPtr.getVaddr();
    ListNode *next = nextPtr.getVaddr();
    if (index == -1)
        return false;
    if (!removeFromIndex(index))
        return false;
    if (numEntries + next->getNumEntries() < MAX_ENTRIES / 2)
    {
        ListNode *delNode = mergeWithNext(genId);
        return true;
    }
    if (prev != NULL && numEntries + prev->getNumEntries() < MAX_ENTRIES / 2)
    {
        ListNode *delNode = mergeWithPrev(genId);
    }
    return true;
}

bool ListNode ::checkRange(Key_t key)
{
    return min <= key && key < max;
}
bool ListNode ::checkRangeLookup(Key_t key)
{
    int numEntries = getNumEntries();
    int ne = numEntries;
    return min <= key && key <= keyArray[ne - 1].first;
}

bool ListNode::probe(Key_t key)
{
    int keyHash = getKeyFingerPrint(key);
    int index = getKeyIndex(key, keyHash);
    if (index == -1)
        return false;
    else
        return true;
}

bool ListNode::lookup(Key_t key, Val_t &value)
{
    int keyHash = getKeyFingerPrint(key);
    int index = getKeyIndex(key, keyHash);
    if (index == -1)
    {
        return false;
    }
    else
    {
        value = keyArray[index].second;
        return true;
    }
}

void ListNode::print()
{
    int numEntries = getNumEntries();
    printf("numEntries:%d min:%s max :%s\n", numEntries, getMin(), getMax());
#ifdef STRINGKEY
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (bitMap[i])
            printf("%s, ", keyArray[i].first);
    }
#else
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (bitMap[i])
            printf("%lu, ", keyArray[i].first);
    }
#endif
    std::cout << "::" << std::endl;
}

bool ListNode::scan(Key_t startKey, int range, std::vector<Val_t> &rangeVector, uint64_t writeVersion, uint64_t genId)
{
restart:
    ListNode *next = nextPtr.getVaddr();
    if (next == nullptr)
        return true;

    int todo = static_cast<int>(range - rangeVector.size());
    int numEntries = getNumEntries();
    if (todo < 1)
        assert(0 && "ListNode:: scan: todo < 1");
    if (writeVersion > lastScanVersion)
    {
        if (!generatePermuter(writeVersion, genId))
        {
            goto restart;
        }
    }
    uint8_t startIndex = 0;
    if (startKey > min)
        startIndex = permuterLowerBound(startKey);
    for (uint8_t i = startIndex; i < numEntries && todo > 0; i++)
    {
        // rangeVector.push_back((Val_t)(keyArray[permuter[i]].first));  // comment add by hpy
        rangeVector.push_back(keyArray[permuter[i]].second);
        todo--;
    }
    return rangeVector.size() == range;
}

uint8_t ListNode::getKeyFingerPrint(Key_t key)
{
#ifdef STRINGKEY
    uint32_t length = key.size();
    uint32_t hash = length;
    const char *str = key.getData();

    for (uint32_t i = 0; i < length; ++str, ++i)
    {
        hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);
    }
    return (uint8_t)(hash & 0xFF);
#else
    key = (~key) + (key << 18); // key = (key << 18) - key - 1;
    key = key ^ (key >> 31);
    key = (key + (key << 2)) + (key << 4);
    key = key ^ (key >> 11);
    key = key + (key << 6);
    key = key ^ (key >> 22);
    return (uint8_t)(key);
#endif
}

hydra::bitset *ListNode::getBitMap()
{
    return &bitMap;
}

uint8_t *ListNode::getFingerPrintArray()
{
    return fingerPrint;
}

bool ListNode::generatePermuter(uint64_t writeVersion, uint64_t genId)
{
    if (pLock.write_lock(genId) == 0)
    {
        return false;
    }
    if (writeVersion == lastScanVersion)
    {
        pLock.write_unlock();
        return true;
    }
    std::vector<std::pair<Key_t, uint8_t>> copyArray;
    for (uint8_t i = 0; i < MAX_ENTRIES; i++)
    {
        if (bitMap[i])
            copyArray.push_back(std::make_pair(keyArray[i].first, i));
    }
#ifdef STRINGKEY
    std::sort(copyArray.begin(), copyArray.end(), compare2);
#else
    std::sort(copyArray.begin(), copyArray.end());
#endif
    for (uint8_t i = 0; i < copyArray.size(); i++)
    {
        permuter[i] = copyArray[i].second;
    }
    flushToNVM((char *)permuter, L1_CACHE_BYTES);
    smp_wmb();
    lastScanVersion = writeVersion;
    flushToNVM((char *)&lastScanVersion, sizeof(uint64_t));
    smp_wmb();
    pLock.write_unlock();
    return true;
}

int ListNode ::permuterLowerBound(Key_t key)
{
    int lower = 0;
    int numEntries = getNumEntries();
    int upper = numEntries;
    do
    {
        int mid = ((upper - lower) / 2) + lower;
        int actualMid = permuter[mid];
        if (key < keyArray[actualMid].first)
        {
            upper = mid;
        }
        else if (key > keyArray[actualMid].first)
        {
            lower = mid + 1;
        }
        else
        {
            return mid;
        }
    } while (lower < upper);
    return (uint8_t)lower;
}
pptr<ListNode> ListNode::recoverSplit(OpStruct *olog)
{
    uint8_t keyHash = getKeyFingerPrint(olog->newKey);
    pptr<ListNode> newNodePtr = split(olog->newKey, olog->newVal, keyHash, 0);
    return newNodePtr;
}

void ListNode::recoverNode(Key_t min_key)
{
    // delete duplicate entries
    hydra::bitset curBitMap = *(this->getBitMap());
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        if (min_key <= keyArray[i].first)
        {
            curBitMap.reset(i);
        }
    }
    bitMap = curBitMap;
    flushToNVM((char *)this, L1_CACHE_BYTES);
    smp_wmb();
}

void ListNode::recoverMergingNode(ListNode *deleted_node)
{
    hydra::bitset curBitMap = *(this->getBitMap());
    bool exist = false;
    int idx = -1;
    for (int i = 0; i < MAX_ENTRIES; i++)
    {
        exist = false;
        idx = -1;
        if (curBitMap[i])
        {
            for (int j = 0; j < MAX_ENTRIES; j++)
            {
                if (deleted_node->getKeyArray()[i].first == keyArray[j].first)
                {
                    exist = true;
                    break;
                }
            }
        }
        else if (idx == -1)
        {
            idx = i;
        }
        if (!exist)
        {
            Key_t key = deleted_node->getKeyArray()[i].first;
            Val_t val = deleted_node->getKeyArray()[i].second;
            uint8_t keyHash = getKeyFingerPrint(key);
            insertAtIndex(std::make_pair(key, val), (uint8_t)idx, keyHash, true);
        }
    }
}
