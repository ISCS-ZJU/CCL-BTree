// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef pactree_COMBINER_H
#define pactree_COMBINER_H
#include "Oplog.h"
#include "WorkerThread.h"
#include <climits>
#include <libpmemobj.h>


class CombinerThread {
private:
    std::queue<std::pair<unsigned long, std::vector<OpStruct *>*>> logQueue;
    unsigned long doneCountCombiner;

public:
    CombinerThread () { doneCountCombiner = 0; }
    std::vector<OpStruct *>* combineLogs() {
        std::atomic_fetch_add(&curQ, 1ul);
        int qnum = static_cast<int>((curQ - 1) % 2);
        auto mergedLog = new std::vector<OpStruct *>;
        for (auto &i : g_perThreadLog) {
            Oplog &log = *i;
            log.lock(qnum);
            auto op_ = log.getQ(qnum);
            if(!op_->empty())
                mergedLog->insert(std::end(*mergedLog), std::begin(*op_), std::end(*op_));
            log.resetQ(qnum);
            log.unlock(qnum);
        }
        std::sort(mergedLog->begin(), mergedLog->end());
        combinerSplits +=mergedLog->size();
        if (!mergedLog->empty()) {
            logQueue.push(make_pair(doneCountCombiner, mergedLog));
            doneCountCombiner++;
        #ifdef MEMORY_FOOTPRINT
            dram_allocated += (mergedLog->size()*sizeof(OpStruct *));
        #endif
            return mergedLog;
        } else {
            delete mergedLog;
            return nullptr;
        }
    }

    void broadcastMergedLog(std::vector<OpStruct *>* mergedLog, int activeNuma) {
        for(auto i = 0; i < activeNuma * WORKER_THREAD_PER_NUMA; i++)
            g_workQueue[i].push(mergedLog);
    }

    uint64_t freeMergedLogs(int activeNuma, bool force) {
        if (!force && logQueue.size() < 100) return 0;

        unsigned long minDoneCountWt = ULONG_MAX;
        for (auto i = 0; i < activeNuma * WORKER_THREAD_PER_NUMA; i++) {
            unsigned long logDoneCount = g_WorkerThreadInst[i]->getLogDoneCount();
            if (logDoneCount < minDoneCountWt)
                minDoneCountWt = logDoneCount;
        }

        while (!logQueue.empty() && logQueue.front().first < minDoneCountWt) {
            auto mergedLog = logQueue.front().second;
            for (auto opsPtr : *mergedLog){
            //    PMEMoid ptr = pmemobj_oid(opsPtr);
            //    pmemobj_free(&ptr);
            //    ptr = pmemobj_oid(mergedLog);
             //   pmemobj_free(&ptr);
	    }
      //          delete opsPtr;

            delete mergedLog;
            logQueue.pop();
        }
        return minDoneCountWt;
    }

    bool mergedLogsToBeFreed() { return logQueue.empty(); }
};

#endif 
