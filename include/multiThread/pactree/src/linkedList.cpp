// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include <climits>
#include <iostream>
#include <cassert>
#include "linkedList.h"
#include "SearchLayer.h"
//std::atomic<int> numSplits;
std::atomic<uint64_t> dists[5];
extern std::mutex mtx[112];

void printDists(){
	uint64_t sum =0 ;
	for(int i=0; i<5; i++){
		printf("%lu \n",dists[i].load());
		sum+=dists[i].load();
	}
	for(int i=0; i<5; i++){
		printf("%lf \%\n",(double)(dists[i].load())/(double)sum*100.0);
		
	}
}

std::atomic<unsigned long> pmemAmount;

void zeroMemAmount(){
	pmemAmount.store(0);
}

void addMemAmount(unsigned long amt){
	pmemAmount+=amt;
}
void printMemAmount(){
	printf("amount :%lu\n",pmemAmount.load());
}

ListNode* LinkedList::initialize() {
    genId =0;
    OpStruct* oplog = (OpStruct *)PMem::getOpLog(0);

    PMem::alloc(1,sizeof(ListNode),(void **)&headPtr,&(oplog->newNodeOid));
    ListNode* head = (ListNode*)new(headPtr.getVaddr()) ListNode();
    flushToNVM((char*)&headPtr,sizeof(pptr<ListNode>));
    smp_wmb();

    OpStruct *oplog2 = (OpStruct *)PMem::getOpLog(1);

    pptr<ListNode> tailPtr;
    PMem::alloc(1,sizeof(ListNode),(void **)&tailPtr, &(oplog->newNodeOid));
    ListNode* tail= (ListNode*)new(tailPtr.getVaddr()) ListNode();

    oplog->op=OpStruct::done;
    oplog2->op=OpStruct::done;

    pptr<ListNode >nullPtr(0,0);

    head->setNext(tailPtr);
    head->setPrev(nullPtr);
    head->setCur(headPtr);
#ifdef STRINGKEY
	std::string minString= "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
    std::string maxString= "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
    Key_t max;
    max.setFromString(maxString);
    Key_t min;
    min.setFromString(minString);
	head->setMin(min);
    head->insert(min,0,0);
    tail->insert(max, 0,0);
	tail->setMin(max);
    head->setMax(max);
#else
    head->insert(0,0,0);
    head->setMin(0);
    head->setMax(ULLONG_MAX);
    tail->insert(ULLONG_MAX, 0,0);
    tail->setMin(ULLONG_MAX);
#endif

//    tail->setNumEntries(MAX_ENTRIES); //This prevents merge of tail
    tail->setFullBitmap(); 
    tail->setNext(nullPtr);
    tail->setPrev(headPtr);
    tail->setCur(tailPtr);


    flushToNVM((char*)head,sizeof(ListNode));
    flushToNVM((char*)tail,sizeof(ListNode));
    smp_wmb();

    for(int i=0; i<5;i++){
	dists[i].store(0);
    }

    return head;
}
#ifdef SYNC
bool LinkedList::insert(Key_t key, Val_t value, ListNode* head,void **locked_parent_node) {
	int retryCount = 0;
restart:
	ListNode* cur = head;
	while (1) {
		if (cur->getMin() > key) {
			cur = cur->getPrev();
			continue;
		}
		if (!cur->checkRange(key)) {
			cur = cur->getNext();
			continue;
		}
		break;
	}
    if (!cur->writeLock(genId)){
		goto restart;
	}
    if (cur->getDeleted()) {
        cur->writeUnlock();
        goto restart;
    }
	if (!cur->checkRange(key)) {
		cur->writeUnlock();
		goto restart;
	}
	bool ret = cur->insert(key, value,locked_parent_node);
	return ret;
}

#endif



bool LinkedList::insert(Key_t key, Val_t value, ListNode* head, int threadId) {
    restart:
    ListNode* cur = head;

	int dist = 0;
    while (1) {
        assert(cur != nullptr && "cur is nullptr!!!\n");
        if (cur->getMin() > key) {
#ifdef DIST
			dist++;
#endif
            if (!cur->getPrev())
                break;
            cur = cur->getPrev();
            continue;
        }
        if (!cur->checkRange(key)) {
#ifdef DIST
			dist++;
#endif
            if (!cur->getNext())
                break;
            cur = cur->getNext();
            continue;
        }
        break;
    }
    // printf("Debug: exit while loop\n");
    if (!cur->writeLock(genId))
        goto restart;
    if (cur->getDeleted()) {
        cur->writeUnlock();
        goto restart;
    }
    if (!cur->checkRange(key)) {
        cur->writeUnlock();
        goto restart;
    }
    // printf("Debug: After traversing to right node\n");

#ifdef DIST
	if(dist==0) dists[0]++;
	else if(dist<2) dists[1]++;
	else if(dist<5) dists[2]++;
	else if(dist<50) dists[3]++;
	else dists[4]++;
#endif

    bool ret = cur->insert(key, value,threadId);
    cur->writeUnlock();
    // printf("Debug: After insert into ListNode\n");
    return ret;
}

bool LinkedList::update(Key_t key, Val_t value, ListNode* head) {
    restart:
    ListNode* cur = head;

    while (1) {
        if (cur->getMin() > key) {
            cur = cur->getPrev();
            continue;
        }
        if (!cur->checkRange(key)) {
            cur = cur->getNext();
            continue;
        }
        break;
    }

    if (!cur->writeLock(genId))
        goto restart;
    if (cur->getDeleted()) {
        cur->writeUnlock();
        goto restart;
    }
    if (!cur->checkRange(key)) {
        cur->writeUnlock();
        goto restart;
    }
    bool ret = cur->update(key, value);
    cur->writeUnlock();
    return ret;
}

bool LinkedList::remove(Key_t key, ListNode *head) {
    restart:
    ListNode* cur = head;

    while (1) {
        if (cur->getMin() > key) {
            cur = cur->getPrev();
            continue;
        }
        if (!cur->checkRange(key)) {
            cur = cur->getNext();
            continue;
        }
        break;
    }
    if (!cur->writeLock(genId)) {
        if (head->getPrev() != nullptr) head = head->getPrev();
        goto restart;
    }
    if (cur->getDeleted()) {
        if (head->getPrev() != nullptr) head = head->getPrev();
        cur->writeUnlock();
        goto restart;
    }
    if (!cur->checkRange(key)) {
        if (head->getPrev() != nullptr) head = head->getPrev();
        cur->writeUnlock();
        goto restart;
    }

    bool ret = cur->remove(key,genId);
    cur->writeUnlock();
    return ret;
}

bool LinkedList::probe(Key_t key, ListNode *head) {
    restart:
    ListNode* cur = head;

    while (1) {
        if (cur->getMin() > key) {
            cur = cur->getPrev();
            continue;
        }
        if (!cur->checkRange(key)) {
            cur = cur->getNext();
            continue;
        }
        break;
    }

    version_t readVersion = cur->readLock(genId);
    //Concurrent Update
    if (!readVersion)
        goto restart;
    if (cur->getDeleted()){
        goto restart;
    }
    if (!cur->checkRange(key)) {
        goto restart;
    }
    bool ret = false;
    ret = cur->probe(key);
    if (!cur->readUnlock(readVersion))
        goto restart;
    return ret;
}

bool LinkedList::lookup(Key_t key, Val_t &value, ListNode *head) {
    restart:
    ListNode* cur = head;
    int count = 0;

    while (1) {
        if (cur->getMin() > key) {
            cur = cur->getPrev();
            continue;
        }
        if (!cur->checkRange(key)) {
            cur = cur->getNext();
            continue;
        }
        break;
    }
    version_t readVersion = cur->readLock(genId);
    //Concurrent Update
    if (!readVersion)
        goto restart;
    if (cur->getDeleted()){
        goto restart;
    }
    if (!cur->checkRange(key)) {
        goto restart;
    }
    bool ret = false;
    ret = cur->lookup(key, value);
    if (!cur->readUnlock(readVersion))
        goto restart;
    return ret;
}

void LinkedList::print(ListNode *head) {
    ListNode* cur = head;
    while (cur->getNext() != nullptr) {
        cur->print();
        cur = cur->getNext();
    }
    std::cout << "\n";
    return;
}

uint32_t LinkedList::size(ListNode *head) {
    ListNode* cur = head;
    int count = 0;
    while (cur->getNext() != nullptr) {
        count++;
        cur = cur->getNext();
    }
    return count;
}

ListNode *LinkedList::getHead() {
    ListNode* head = (ListNode*) headPtr.getVaddr();
    return head;
}

uint64_t LinkedList::scan(Key_t startKey, int range, std::vector<Val_t> &rangeVector, ListNode *head) {
    restart:
    ListNode* cur = head;
    rangeVector.clear();
    // Find the start Node
    while (1) {
        if (cur->getMin() > startKey) {
            cur = cur->getPrev();
            continue;
        }
        if (!cur->checkRange(startKey)) {
            cur = cur->getNext();
            continue;
        }
        break;
    }
    bool end = false;
    assert(rangeVector.size() == 0);
	std::vector<Val_t> resultBuffer;
    while (rangeVector.size() < range && !end) {
        version_t readVersion = cur->readLock(genId);
        //Concurrent Update
        if (!readVersion)
         	continue;
		/*resultBuffer.clear();*/
	if (cur->getDeleted())
           goto restart;
        end = cur->scan(startKey, range, rangeVector, readVersion,genId);
        if(!cur->readUnlock(readVersion)){
			continue;
		}
        cur = cur->getNext();
    }
    return rangeVector.size();
}

bool LinkedList::Recovery(void* p_sl) {
   genId+=1;
   SearchLayer *sl = (SearchLayer *)p_sl;
   int i=0;
   for(i=0; i<1000*112; i++){
       OpStruct *oplog = (OpStruct*)PMem::getOpLog(i); 
       if((oplog->op == OpStruct::insert)){
           pptr<ListNode> node;
           pptr<ListNode> next_node;
           node.setRawPtr(oplog->oldNodePtr);  
	   next_node = node->getNextPtr();
           if((next_node.getVaddr()!=pmemobj_direct(oplog->newNodeOid))){
                printf("case 1\n");
		// not connected
		pmemobj_free(&oplog->newNodeOid);
                next_node = node->recoverSplit(oplog);
           }
	   else{
               node->recoverNode(oplog->key);
                if(sl->isEmpty()){
                    sl->insert(oplog->key,oplog->oldNodePtr);
                }
                else if(sl->lookup(oplog->key)!=oplog->oldNodePtr){
                    sl->insert(oplog->key,oplog->oldNodePtr);
                }
           }
           ListNode* nnextNode = next_node->getNext();
           nnextNode->setPrev(next_node);
           oplog->op=OpStruct::done;
       }
       else if((oplog->op == OpStruct::remove)){
           pptr<ListNode> node; // deleting node
           pptr<ListNode> prev_node;
           node.setRawPtr(oplog->oldNodePtr);   
	   prev_node = node->getPrevPtr();
           prev_node->recoverMergingNode(node.getVaddr());
           if(sl->isEmpty()){
		;
           }
           else if(sl->lookup(oplog->key)==(void*)node.getRawPtr()){
               sl->remove(oplog->key,(void*)node.getRawPtr());
           }
           pmemobj_free(&oplog->newNodeOid);
       }
   }
}
