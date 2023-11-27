// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include <iostream>
#include <zconf.h>
#include <cassert>
#include "numa.h"
#include "pactreeImpl.h"
#include "numa-config.h"
#include "Combiner.h"
#include "WorkerThread.h"
#include <ordo_clock.h>

#ifdef PACTREE_ENABLE_STATS
#define acc_sl_time(x) (curThreadData->sltime += x)
#define acc_dl_time(x) (curThreadData->dltime += x)
#define hydralist_reset_timers() \
    total_sl_time = 0;           \
    total_dl_time = 0;
#define hydralist_start_timer()      \
    {                                \
        unsigned long __b_e_g_i_n__; \
        __b_e_g_i_n__ = read_tsc();
#define hydralist_stop_timer(tick)        \
    (tick) += read_tsc() - __b_e_g_i_n__; \
    }
#else
#define acc_sl_time(x)
#define acc_dl_time(x)
#define hydralist_start_timer()
#define hydralist_stop_timer(tick)
#define hydralist_reset_timers()
#endif

std::vector<WorkerThread *> g_WorkerThreadInst(MAX_NUMA *WORKER_THREAD_PER_NUMA);
std::vector<SearchLayer *> g_perNumaSlPtr(MAX_NUMA);
std::set<ThreadData *> g_threadDataSet;
std::atomic<bool> g_globalStop;
std::atomic<bool> g_combinerStop;
thread_local int pactreeImpl::threadNumaNode = -1;
thread_local ThreadData *curThreadData = NULL;
int pactreeImpl::totalNumaActive = 0;
volatile bool threadInitialized[MAX_NUMA];
volatile bool slReady[MAX_NUMA];
std::mutex g_threadDataLock;
uint64_t g_removeCount;
volatile std::atomic<bool> g_removeDetected;

void *PMem::baseAddresses[] = {};
void *PMem::logVaddr[] = {};

void workerThreadExec(int threadId, int activeNuma, root_obj *root)
{
    WorkerThread wt(threadId, activeNuma);
    g_WorkerThreadInst[threadId] = &wt;
    if (threadId < activeNuma)
    {
        while (threadInitialized[threadId] == 0)
            ;
        slReady[threadId] = false;
        //        assert(numa_node_of_cpu(sched_getcpu()) == threadId);
        g_perNumaSlPtr[threadId] = pactreeImpl::createSearchLayer(root, threadId);
        g_perNumaSlPtr[threadId]->setNuma(threadId);
        slReady[threadId] = true;
    }
    int count = 0;
    uint64_t lastRemoveCount = 0;
    while (!g_combinerStop)
    {
        usleep(200);
        while (!wt.isWorkQueueEmpty())
        {
            count++;
            wt.applyOperation();
        }
        if (threadId == 0 && lastRemoveCount != g_removeCount)
        {
            //            wt.freeListNodes(g_removeCount);
        }
    }
    while (!wt.isWorkQueueEmpty())
    {
        count++;
        if (wt.applyOperation() && !g_removeDetected)
        {
            g_removeDetected.store(true);
        }
    }
    // If worker threads are stopping that means there are no more
    // user threads
    if (threadId == 0)
    {
        wt.freeListNodes(ULLONG_MAX); // add  by zhenxin
    }
    g_WorkerThreadInst[threadId] = NULL;

    printf("Worker thread: %d Ops: %d\n", threadId, wt.opcount);
}

uint64_t gracePeriodInit(std::vector<ThreadData *> &threadsToWait)
{
    uint64_t curTime = ordo_get_clock();
    for (auto td : g_threadDataSet)
    {
        if (td->getFinish())
        {
            g_threadDataLock.lock();
            g_threadDataSet.erase(td);
            g_threadDataLock.unlock();
            free(td);
            continue;
        }
        if (td->getRunCnt() % 2)
        {
            if (ordo_gt_clock(td->getLocalClock(), curTime))
                continue;
            else
                threadsToWait.push_back(td);
        }
    }
    return curTime;
}
void waitForThreads(std::vector<ThreadData *> &threadsToWait, uint64_t gpStartTime)
{
    for (int i = 0; i < threadsToWait.size(); i++)
    {
        if (threadsToWait[i] == NULL)
            continue;
        ThreadData *td = threadsToWait[i];
        if (td->getRunCnt() % 2 == 0)
            continue;
        if (ordo_gt_clock(td->getLocalClock(), gpStartTime))
            continue;
        while (td->getRunCnt() % 2)
        {
            usleep(1);
            std::atomic_thread_fence(std::memory_order::memory_order_acq_rel);
        }
    }
}
void broadcastDoneCount(uint64_t removeCount)
{
    g_removeCount = removeCount;
}
void combinerThreadExec(int activeNuma)
{
    CombinerThread ct;
    int count = 0;
    while (!g_globalStop)
    {
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr)
        {
            count++;
            ct.broadcastMergedLog(mergedLog, activeNuma);
        }
        uint64_t doneCountWt = ct.freeMergedLogs(activeNuma, false);
        std::vector<ThreadData *> threadsToWait;
        if (g_removeDetected && doneCountWt != 0)
        {
            uint64_t gpStartTime = gracePeriodInit(threadsToWait);
            waitForThreads(threadsToWait, gpStartTime);
            broadcastDoneCount(doneCountWt);
            g_removeDetected.store(false);
        }
        else
            usleep(100);
    }
    // TODO fix this
    int i = 20;
    while (i--)
    {
        usleep(200);
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr)
        {
            count++;
            ct.broadcastMergedLog(mergedLog, activeNuma);
        }
    }
    while (!ct.mergedLogsToBeFreed())
        ct.freeMergedLogs(activeNuma, true);
    g_combinerStop = true;
    printf("Combiner thread: Ops: %d\n", count);
}

void pinThread(std::thread *t, int numaId)
{
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    for (int i = 0; i < NUM_SOCKET; i++)
    {
        for (int j = 0; j < SMT_LEVEL; j++)
        {
            CPU_SET(OS_CPU_ID[numaId][i][j], &cpuSet);
        }
    }
    int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuSet);
    assert(rc == 0);
}
int cnt = 0;
void pactreeImpl::createWorkerThread(int numNuma, root_obj *root)
{
    for (int i = 0; i < numNuma * WORKER_THREAD_PER_NUMA; i++)
    {
        threadInitialized[i % numNuma] = false;
        std::thread *wt = new std::thread(workerThreadExec, i, numNuma, root);
        printf("Worker thread created!\n");
        usleep(1);
        wtArray->push_back(wt);
        // pinThread(wt, i % numNuma);
        threadInitialized[i % numNuma] = true;
    }
}

void pactreeImpl::createCombinerThread()
{
    combinerThead = new std::thread(combinerThreadExec, totalNumaActive);
    printf("Combiner thread created!\n");
}

pactreeImpl *initPT(int numa)
{
    // node 0

    auto path = *pool_dir_ + "/dl";
    // size_t sz = 32UL*1024UL*1024UL*1024UL; //32GB
    int isCreated = 0;
    int isCreated2 = 0;
    root_obj *root = nullptr;
    root_obj *sl_root = nullptr;

    auto sl_path = *pool_dir_ + "/sl";
    // size_t sl_size = 32UL*1024UL*1024UL*1024UL;

    PMem::bind(0, sl_path.c_str(), pool_size_, (void **)&sl_root, &isCreated);
    if (isCreated == 0)
    {
        printf("Reading Search layer from an existing pactree.\n");
    }
    auto log_path = *pool_dir_ + "/log";
    PMem::bindLog(0, log_path.c_str(), pool_size_);

    PMem::bind(1, path.c_str(), pool_size_, (void **)&root, &isCreated2);

#ifdef MULTIPOOL
    // node 1
    char path2[100], sl_path2[100], log_path2[100];
    strcpy(path2, NVM_FILE_PATH0);
    strcpy(sl_path2, NVM_FILE_PATH0);
    strcpy(log_path2, NVM_FILE_PATH0);

    strcat(path2,"dl");
    strcat(sl_path2,"sl");
    strcat(log_path2,"log");

    // const char *path2 = "xx/dl";
    // const char *sl_path2 = "xx/sl";
    // const char *log_path2 = "xx/log";

    root_obj *root2 = nullptr;
    root_obj *sl_root2 = nullptr;
    PMem::bind(3, sl_path2, pool_size_, (void **)&sl_root2, &isCreated);
    PMem::bind(4, path2, pool_size_, (void **)&root2, &isCreated);
    PMem::bindLog(1, log_path2, pool_size_);
#endif
    if (isCreated2 == 0)
    {
        pactreeImpl *pt = (pactreeImpl *)pmemobj_direct(root->ptr[0]);
        pt->init(numa, sl_root);
        return pt;
    }
    PMEMobjpool *pop = (PMEMobjpool *)PMem::getBaseOf(1);

    int ret = pmemobj_alloc(pop, &(root->ptr[0]), sizeof(pactreeImpl), 0, NULL, NULL);
#ifdef MEMORY_FOOTPRINT
    pmem_allocated += sizeof(pactreeImpl);
#endif
    void *rootVaddr = pmemobj_direct(root->ptr[0]);
    pactreeImpl *pt = (pactreeImpl *)new (rootVaddr) pactreeImpl(numa, sl_root);
    flushToNVM((char *)root, sizeof(root_obj));
    smp_wmb();
    return pt;
}

void pactreeImpl::init(int numNuma, root_obj *root)
{
    int id = 0;

    assert(numNuma <= NUM_SOCKET);
    totalNumaActive = numNuma;
    wtArray = new std::vector<std::thread *>(totalNumaActive);
    g_WorkerThreadInst.clear();
    g_perNumaSlPtr.clear();
    g_threadDataSet.clear();

    g_perNumaSlPtr.resize(totalNumaActive);
    g_globalStop = false;
    g_combinerStop = false;
    createWorkerThread(numNuma, root);
    createCombinerThread();

    for (int i = 0; i < totalNumaActive; i++)
    {
        while (slReady[i] == false)
            ;
    }
    recover();
    hydralist_reset_timers();
}

pactreeImpl::pactreeImpl(int numNuma, root_obj *root)
{

    assert(numNuma <= NUM_SOCKET);
    totalNumaActive = numNuma;
    wtArray = new std::vector<std::thread *>(totalNumaActive);
    g_perNumaSlPtr.resize(totalNumaActive);
    dl.initialize();
    // need to read from PM

    g_globalStop = false;
    g_combinerStop = false;
#ifdef SYNC
    totalNumaActive = 1;
    g_perNumaSlPtr.resize(totalNumaActive);
    g_perNumaSlPtr[0] = pactreeImpl::createSearchLayer(root, 0);
#else
    createWorkerThread(numNuma, root);
    createCombinerThread();
    for (int i = 0; i < totalNumaActive; i++)
    {
        while (slReady[i] == false)
            ;
    }
#endif
    hydralist_reset_timers();
}

pactreeImpl::~pactreeImpl()
{
    g_globalStop = true;
    for (auto &t : *wtArray)
        t->join();
    combinerThead->join();

    std::cout << "Total splits: " << numSplits << std::endl;
    std::cout << "Combiner splits: " << combinerSplits << std::endl;
#ifdef PACTREE_ENABLE_STATS
    std::cout << "total_dl_time: " << total_dl_time / numThreads / 1000 << std::endl;
    std::cout << "total_sl_time: " << total_sl_time / numThreads / 1000 << std::endl;
#endif
}

ListNode *pactreeImpl::getJumpNode(Key_t &key)
{
    int numaNode = getThreadNuma();
    SearchLayer &sl = *g_perNumaSlPtr[numaNode];
    if (sl.isEmpty())
    {
        return dl.getHead();
    }
    ListNode *jumpNode = reinterpret_cast<ListNode *>(sl.lookup(key));
    //    idxPtr2 jumpNode = reinterpret_cast<idxPtr2>(sl.lookup(key));
    if (jumpNode == nullptr)
        jumpNode = dl.getHead();
    return jumpNode;
}

#ifdef SYNC

// lock interface
ListNode *pactreeImpl::getJumpNodewithLock(Key_t &key, void **node)
{
    SearchLayer &sl = *g_perNumaSlPtr[0];
    if (sl.isEmpty())
        return dl.getHead();
    auto *jumpNode = reinterpret_cast<ListNode *>(sl.lookupwithLock(key, node));
    if (jumpNode == nullptr)
    {
        jumpNode = dl.getHead();
    }
    return jumpNode;
}

// unlock interface
bool pactreeImpl::JumpNodewithUnLock(void *node)
{
    SearchLayer &sl = *g_perNumaSlPtr[0];

    return sl.nodeUnlock(node);
}
#endif

bool pactreeImpl::insert(Key_t &key, Val_t val)
{
    // std::cout << "key = " << key <<std::endl;
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);
    uint64_t ticks = 0;

    int threadId = curThreadData->getThreadId();
    bool ret;
    hydralist_start_timer();
    ListNode *jumpNode;
#ifdef SYNC
    void *art_node = nullptr;
    jumpNode = getJumpNodewithLock(key, &art_node);
#else
    jumpNode = getJumpNode(key);
#endif
    // printf("Debug: After getJumpNode\n");
    // hydralist_stop_timer(ticks);
    hydralist_start_timer();
    acc_sl_time(ticks);
#ifndef SYNC
    ret = dl.insert(key, val, jumpNode, threadId);
#else
    ret = dl.insert(key, val, jumpNode, &art_node);
    if (art_node != nullptr)
    {
        JumpNodewithUnLock(art_node);
    }
#endif
    // printf("Debug: After dl.insert\n");

    hydralist_stop_timer(ticks);
    acc_dl_time(ticks);
    curThreadData->read_unlock();
    // unregisterThread();
    return ret;
}

bool pactreeImpl::update(Key_t &key, Val_t val)
{
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);

    ListNode *jumpNode = getJumpNode(key);

    bool ret = dl.update(key, val, jumpNode);
    curThreadData->read_unlock();
    // unregisterThread();
    return ret;
}

Val_t pactreeImpl::lookup(Key_t &key)
{

    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);
    Val_t val;
    uint64_t ticks;
    // hydralist_start_timer();

    ListNode *jumpNode = getJumpNode(key);
    // hydralist_stop_timer(ticks);
    // acc_sl_time(ticks);

    // hydralist_start_timer();
    //
    dl.lookup(key, val, jumpNode);
    // hydralist_stop_timer(ticks);
    // acc_dl_time(ticks);
    curThreadData->read_unlock();
    // unregisterThread();
    return val;
}

bool pactreeImpl::remove(Key_t &key)
{
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);

    ListNode *jumpNode = getJumpNode(key);

    bool ret = dl.remove(key, jumpNode);
    curThreadData->read_unlock();
    // unregisterThread();
    return ret;
}

SearchLayer *pactreeImpl::createSearchLayer(root_obj *root, int threadId)
{
    if (pmemobj_direct(root->ptr[threadId]) == nullptr)
    {
        pptr<SearchLayer> sPtr;
        PMem::alloc(0, sizeof(SearchLayer), (void **)&sPtr, &(root->ptr[threadId]));
        SearchLayer *s = new (sPtr.getVaddr()) SearchLayer();
        return s;
    }
    else
    {
        SearchLayer *s = (SearchLayer *)pmemobj_direct(root->ptr[threadId]);
        s->init();
        return s;
    }

    // Read from Root Object or Allocate new one.
}

unsigned long tacc_rdtscp(int *chip, int *core)
{
    unsigned long a, d, c;
    __asm__ volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
    *chip = (c & 0xFFF000) >> 12;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << 32);
    ;
}

int pactreeImpl::getThreadNuma()
{
    int chip;
    int core;
    if (threadNumaNode == -1)
    {
        tacc_rdtscp(&chip, &core);
        threadNumaNode = chip;
        if (threadNumaNode > 8)
            assert(0);
    }
    if (totalNumaActive <= threadNumaNode)
        return 0;
    else
        return threadNumaNode;
}

uint64_t pactreeImpl::scan(Key_t &startKey, int range, std::vector<Val_t> &result)
{
    ListNode *jumpNode = getJumpNode(startKey);

    return dl.scan(startKey, range, result, jumpNode);
}

void pactreeImpl::registerThread()
{
    int threadId = numThreads.fetch_add(1);
    auto td = new ThreadData(threadId);
    g_threadDataLock.lock();
    g_threadDataSet.insert(td);
    g_threadDataLock.unlock();
    curThreadData = td;
    std::atomic_thread_fence(std::memory_order_acq_rel);
}

void pactreeImpl::unregisterThread()
{
    if (curThreadData == NULL)
        return;
    int threadId = curThreadData->getThreadId();
    curThreadData->setfinish();
#ifdef PACTREE_ENABLE_STATS
    total_dl_time.fetch_add(curThreadData->dltime);
    total_sl_time.fetch_add(curThreadData->sltime);
#endif
}

void pactreeImpl::recover()
{
    dl.Recovery(g_perNumaSlPtr[0]);
}
