// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include "Oplog.h"
#include "ordo_clock.h"
#include "pptr.h"
#include <iostream>

std::set<Oplog*> g_perThreadLog;
boost::lockfree::spsc_queue<std::vector<OpStruct *>*, boost::lockfree::capacity<10000>> g_workQueue[MAX_NUMA * WORKER_THREAD_PER_NUMA];
thread_local Oplog* Oplog::perThreadLog;
std::atomic<int> numSplits;
int combinerSplits = 0;
std::atomic<unsigned long> curQ;
Oplog::Oplog() {
}

void Oplog::enq(OpStruct *ops) {
#ifdef SYNC
	unsigned long qnum = 0;
#else
    unsigned long qnum = curQ % 2;
    while (!qLock[qnum].try_lock()) {
        std::atomic_thread_fence(std::memory_order_acq_rel);
        qnum = curQ % 2;
    }
#endif
    op_[qnum].push_back(ops);
#ifndef SYNC
   qLock[qnum].unlock();
#endif
    //std::atomic_fetch_add(&numSplits, 1);
}


void Oplog::enq(OpStruct::Operation op, Key_t key, uint8_t hash, void* newNodePtr) {
    OpStruct* ops = new OpStruct;
#ifdef MEMORY_FOOTPRINT
    dram_allocated += sizeof(OpStruct);
#endif
    ops->op = op;
    ops->key = key;
    ops->hash = static_cast<uint8_t>(hash % WORKER_THREAD_PER_NUMA);
    //ops->newNodePtr = newNodePtr;
    ops->ts = ordo_get_clock();
#ifdef SYNC
	unsigned long qnum = 0;
#else
    unsigned long qnum = curQ % 2;
    while (!qLock[qnum].try_lock()) {
        std::atomic_thread_fence(std::memory_order_acq_rel);
        qnum = curQ % 2;
    }
#endif
    op_[qnum].push_back(ops);
#ifndef SYNC
    qLock[qnum].unlock();
#endif
    //std::atomic_fetch_add(&numSplits, 1);
}

void Oplog::resetQ(int qnum) {
    op_[qnum].clear();
}


std::vector<OpStruct*>* Oplog::getQ(int qnum) {
    return &op_[qnum];
}


Oplog* Oplog::getOpLog() {
    Oplog* perThreadLog = Oplog::getPerThreadInstance();
    if(!g_perThreadLog.count(perThreadLog)) {
        perThreadLog = new Oplog;
    #ifdef MEMORY_FOOTPRINT
        dram_allocated += sizeof(Oplog);
    #endif
        g_perThreadLog.insert(perThreadLog);
        setPerThreadInstance(perThreadLog);
    }
    return perThreadLog;
}
void Oplog::enqPerThreadLog(OpStruct::Operation op, Key_t key, uint8_t hash, void* newNodePtr) {
    Oplog* perThreadLog = getOpLog();
    perThreadLog->enq(op, key, hash, newNodePtr);
}

void Oplog::enqPerThreadLog(OpStruct* ops){
    Oplog* perThreadLog = getOpLog();
    perThreadLog->enq(ops);
}

void Oplog::writeOpLog(OpStruct *oplog, OpStruct::Operation op, Key_t key, void* oldNodeRawPtr, uint16_t poolId, Key_t newKey, Val_t newVal){
     oplog->op= op;
     oplog->key = key;
     oplog->oldNodePtr = oldNodeRawPtr; //should be persistent ptr
     oplog->poolId = poolId;
     oplog->newKey = newKey;
     oplog->newVal= newVal;
}

