// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef _LISTNODE_H
#define _LISTNODE_H

#include <vector>
#include "bitset.h"
#include "VersionedLock.h"
#include "common.h"
#include "arch.h"
#include "ordo_clock.h"
#include "../lib/PDL-ART/Tree.h"
#include <mutex>
#include <immintrin.h>

//#define MAX_ENTRIES 32 
#define MAX_ENTRIES 64 
//#define MAX_ENTRIES 128 
class ListNode
{
private:
    Key_t min; // 8B
    Key_t max; //8B
//    uint8_t numEntries; //1B
    pptr<ListNode> curPtr;
    pptr<ListNode> nextPtr;
    pptr<ListNode> prevPtr;
    bool deleted; // 1B
    hydra::bitset bitMap; //8B
    uint8_t fingerPrint[MAX_ENTRIES]; //64B
    VersionedLock verLock;            //8B

    std::pair<Key_t, Val_t> keyArray[MAX_ENTRIES]; // 16*64 = 1024B

    uint64_t lastScanVersion;  // 8B
    VersionedLock pLock;          // ?
    //std::mutex pLock;          // ?
    uint8_t permuter[MAX_ENTRIES]; //64B

    pptr<ListNode> split(Key_t key, Val_t val, uint8_t keyHash, int threadId);
	pptr<ListNode> split(Key_t key, Val_t val, uint8_t keyHash, int threadId, void **olog);
    ListNode* mergeWithNext(uint64_t genId);
    ListNode* mergeWithPrev(uint64_t genId);
    uint8_t getKeyInsertIndex(Key_t key);
    int getKeyIndex(Key_t key, uint8_t keyHash);
    int getFreeIndex(Key_t key, uint8_t keyHash);
    bool insertAtIndex(std::pair<Key_t,Val_t> key, int index, uint8_t keyHash, bool flush);
    bool updateAtIndex(Val_t value, int index);
    bool removeFromIndex(int index);
    int lowerBound(Key_t key);
    uint8_t getKeyFingerPrint(Key_t key);
    bool generatePermuter(uint64_t writeVersion,uint64_t genId);
    int permuterLowerBound(Key_t key);

public:
    ListNode();
#ifdef SYNC
    int insert(Key_t key, Val_t value, void **locked_parent_node);
#endif
    bool insert(Key_t key, Val_t value, int threadId);
    bool update(Key_t key, Val_t value);
    bool remove(Key_t key, uint64_t genId);
    bool probe(Key_t key); //return True if key exists
    bool lookup(Key_t key, Val_t &value);
    bool scan(Key_t startKey, int range, std::vector<Val_t> &rangeVector, uint64_t writeVersion, uint64_t genId);
    void print();
    bool checkRange(Key_t key);
    bool checkRangeLookup(Key_t key);
    version_t readLock(uint64_t genId);
    version_t writeLock(uint64_t genId);
    bool readUnlock(version_t);
    void writeUnlock();
    void setCur(pptr<ListNode> next);
    void setNext(pptr<ListNode> next);
    void setPrev(pptr<ListNode> prev);
    void setMin(Key_t key);
    void setMax(Key_t key);
//    void setNumEntries(int numEntries);
    void setDeleted(bool deleted);
    void setFullBitmap() ;
    ListNode* getNext();
    pptr<ListNode> getNextPtr();
    ListNode* getPrev();
    pptr<ListNode> getPrevPtr();
    pptr<ListNode> getCurPtr();
    int getNumEntries();
    Key_t getMin();
    Key_t getMax();
    std::pair<Key_t, Val_t>* getKeyArray();
    std::pair<Key_t, Val_t>* getValueArray();
    hydra::bitset *getBitMap();
    uint8_t *getFingerPrintArray();
    bool getDeleted();
    void recoverNode(Key_t);
    pptr<ListNode> recoverSplit(OpStruct* olog);
    void recoverMergingNode(ListNode *deleted_node);

};
#endif
