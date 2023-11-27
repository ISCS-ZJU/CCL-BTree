// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef pactree_H
#define pactree_H 

#include <utility>
#include <vector>
#include <algorithm>
#include "common.h"
#include "linkedList.h"
#include <thread>
#include <set>
#include "SearchLayer.h"
#include "threadData.h"

extern size_t pool_size_;
extern std::string *pool_dir_;

extern std::vector<SearchLayer*> g_perNumaSlPtr;
extern std::set<ThreadData*> g_threadDataSet;
typedef LinkedList DataLayer;
class pactreeImpl {
private:
//    SearchLayer sl;
    DataLayer dl;
    std::vector<std::thread*> *wtArray; // CurrentOne but there should be numa number of threads
    std::thread* combinerThead;
    static thread_local int threadNumaNode;
    void createWorkerThread(int numNuma,root_obj *root);
    void createCombinerThread();
    ListNode *getJumpNode(Key_t &key);
    static int totalNumaActive;
    std::atomic<uint32_t> numThreads;

public:
    pptr<SearchLayer> sl;
    explicit pactreeImpl(int numNuma,root_obj *root);
    ~pactreeImpl();
    bool insert(Key_t &key, Val_t val);
    bool update(Key_t &key, Val_t val);
    bool remove(Key_t &key);
    void registerThread();
    void unregisterThread();
    Val_t lookup(Key_t &key);
    void recover();
#ifdef SYNC
    ListNode* getJumpNodewithLock(Key_t &key, void** node);
    bool JumpNodewithUnLock(void* node);
#endif
    uint64_t scan(Key_t &startKey, int range, std::vector<Val_t> &result);
    static SearchLayer* createSearchLayer(root_obj *root, int threadId);
    static int getThreadNuma();
    void init(int numNuma, root_obj* root) ;

#ifdef PACTREE_ENABLE_STATS
    std::atomic<uint64_t> total_sl_time;
    std::atomic<uint64_t> total_dl_time;
#endif
};


pactreeImpl *initPT(int numa);
#endif //pactree_H
