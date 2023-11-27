# CCL-BTree: A Crash-Consistent Locality-Aware B+-Tree for Reducing XPBuffer-Induced Write Amplification in Persistent Memory

## Introduction

CCL-BTree is a persistent B+Tree designed to reduce XBI-amplification, which occurs due to the access granularity mismatch between cacheline and device's internal media (i.g., 256 bytes in Intel Optane DCPMMs). CCL-BTree consists of three techniques including leaf-node centric buffering, write-conservative logging, and locality-aware garbage collection to alleviate the XBI-amplification in all the code paths. 

Please read the following paper for more details:

Zhenxin Li, Shuibing He, Zheng Dang, Peiyi Hong, Xuechen Zhang, Rui Wang, Fei Wu. CCL-BTree: A Crash-Consistent Locality-Aware B+-Tree for Reducing XPBuffer-Induced Write Amplification in Persistent Memory. EuroSys 2024.

## Directories

* include/multiThread: source files for all indexes including DPTree, FAST&FAIR, FPTree, LB+-Tree, PACTree, uTree, and two versions of CCL-BTree.
* include/tools: common files.
* mybin: generated executable files.
* test/multiThread: the wrapper and the test file.

Note: We provide two versions of CCL-BTree, namely CCL-BTree-FF and CCL-BTree-LB, whose DRAM layers follow the implementations of FAST&FAIR and LB+-Tree, respectively. These two versions have similar performance if the thread competition is not severe. Otherwise, the performance of CCL-BTree-LB drops significantly due to frequent HTM transaction aborts.

## Running

The code is designed for machines equipped with Intel Optane DCPMMs.

If you want to evaluate all indexes on a single socket, please change the NVM file path `NVM_FILE_PATH0` in `include/util_normal.h` and execute the script as following:

```
sh m_normal_test.sh [index_name]
```

If you want to run experiments on multiple sockets, please configure parameters in `include/util_normal_numa.h` as following three steps:

1. change the NVM file paths `NVM_FILE_PATH0, NVM_FILE_PATH1`
2. configure NUMA parameters `NUM_NUMA_NODE, NUM_CORE_PER_NUMA`
3. change the id array of cpu cores `socket_all_proc_id` to bind each thread to a specific cpu core

And then you can execute the script as following:

```
sh m_normal_test_numa.sh [index_name]
```
