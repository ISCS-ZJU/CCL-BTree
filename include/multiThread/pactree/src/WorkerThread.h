// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef PACTREE_WORKERTHREAD_H
#define PACTREE_WORKERTHREAD_H
#include "common.h"
#include "Oplog.h"
#include <queue>

class WorkerThread {
private:
    boost::lockfree::spsc_queue<std::vector<OpStruct *>*, boost::lockfree::capacity<10000>> *workQueue;
    int workerThreadId;
    int activeNuma;
    unsigned long logDoneCount;
    std::queue<std::pair<uint64_t, void*>> *freeQueue;
public:
    unsigned long opcount;
    WorkerThread(int id, int activeNuma);
    bool applyOperation();
    bool isWorkQueueEmpty();
    unsigned long getLogDoneCount() {return logDoneCount;}
    void freeListNodes(uint64_t removeCount);
};
extern std::vector<WorkerThread *> g_WorkerThreadInst;


#endif 
