#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <libpmemobj.h>
#include <libpmem.h>
#include <sys/stat.h>
#include <vector>
#include <set>
#include <queue>
#include <future>
#include <assert.h>
#include <random>
#include <utility>
#include <stdint.h>
#include <pthread.h>
#include <shared_mutex>
#include <mutex>
#include <climits>

#include "tools/persist.h"
#include "tools/timer.h"
#include "tools/utils.h"
#include "tools/zipfian_generator.h"
#include "tools/scrambled_zipfian_generator.h"

#include "tools/nodepref.h"
#include <unistd.h>
#include <sstream>

#define key_type_sob int64_t
#define value_type_sob int64_t

#define SHUFFLE_KEYS

/**********************************DEFINE**************************************/
// bind each thread to a fixed CPU core
#define PIN_CPU
// #define PIRNT_PIN_CPU_INFO

// skew test
// #define ZIPFIANDEDINE

// Unified node size,i.e., 256-byte leaf node cotained 14 kvs
// #define UNIFIED_NODE

// overwrite it or just return when find a duplicate key. [defalut open]
#define INSERT_REPEAT_KEY

// the number of merging threads for dptree
inline int parallel_merge_worker_num = 16;
// #define FIXED_BACKGROUND

// eADR test
//  #define eADR_TEST

// dram space test
// #define DRAM_SPACE_TEST

// open or close the entry moving technique for LB+-tree
// #define TREE_NO_BUFFER

// open or close the write-conservative logging technique for CCL-BTree
// #define TREE_NO_SELECLOG

// warm up [default open]
#define DO_WARMUP

// insert [default open]
#define DO_INSERT

// #define DO_UPDATE

// #define DO_SEARCH

// #define DO_SCAN

// #define DO_DELETE

/*****************************************************global variable**********************************/

inline __thread int thread_id;
inline uint64_t num_keys;
inline uint64_t num_threads;

inline HistogramSet *hist_set_group[100];
inline uint64_t elapsed_time_group[100];
inline uint64_t count_log_group[100];
inline uint64_t pre_total_log = 0;
inline uint64_t count_lnode_group[100];

inline uint64_t count_error_insert[100];
inline uint64_t count_error_update[100];
inline uint64_t count_error_search[100];
inline uint64_t count_error_delete[100];
inline uint64_t count_error_scan[100];

inline uint64_t count_conflict_in_bnode[100];

inline uint64_t free_bnode[100];
inline uint64_t create_bnode[100];

inline uint64_t dram_space;

inline int mscan_size = 100;
inline int mmax_length_for_scan = 100 + 64;

// for RTM
#define ABORT_INODE 5
#define ABORT_BNODE 6
#define ABORT_LNODE 7

#define NUM_NUMA_NODE 2
#define NUM_CORE_PER_NUMA 48

/***************************************************************************************/

#ifdef PIN_CPU
#define handle_error_en(en, msg) \
	do                           \
	{                            \
		errno = en;              \
		perror(msg);             \
		exit(EXIT_FAILURE);      \
	} while (0)

// cat /proc/cpuinfo
// to check the number of socket and the cpu id.
// [processor] is the logical core
// [physical id] is the socket
// [core id] is the core id of each socket

/* node information
numactl -H

available: 2 nodes (0-1)
node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71
node 0 size: 64071 MB
node 0 free: 17707 MB
node 1 cpus: 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95
node 1 size: 64499 MB
node 1 free: 20168 MB
node distances:
node   0   1
  0:  10  20
  1:  20  10

*/

// NOTE: mnt/pmem is mounted on the numa node 0, so we should pin all threads to cpu cores in the numa node 0.
const int per_socket_proc_num = 48;
const int socket_0_proc_id[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 48,
								49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71};
const int socket_1_proc_id[] = {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
								47, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95};

const int socket_all_proc_id[] = {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
								  47, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95,
								  0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 48,
								  49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71};

static void pin_cpu_core(int id)
{
	int s, proc_id;
	cpu_set_t cpuset;
	pthread_t thread;

	proc_id = socket_all_proc_id[id];

	thread = pthread_self();
	CPU_ZERO(&cpuset);
	CPU_SET(proc_id, &cpuset);

	s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);

#ifdef PIRNT_PIN_CPU_INFO
	if (s != 0)
	{
		handle_error_en(s, "pthread_setaffinity_np");
		fprintf(stderr, "Failed to pin to CPU %d\n", proc_id);
	}

	/* Check the actual affinity mask assigned to the thread */

	s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
	if (s != 0)
	{
		handle_error_en(s, "pthread_getaffinity_np");
	}

	if (CPU_ISSET(proc_id, &cpuset))
	{
		fprintf(stderr, "Set returned by pthread_getaffinity_np() contained: CPU %d\n", proc_id);
	}
	else
	{
		fprintf(stderr, "Got CPU is not equal to CPU %d\n", proc_id);
	}
#endif
}
#endif

/***************************************************** open pmem file **********************************/
/* extend to numa */
#define NUM_NUMA_NODE 2
#define NUM_CORE_PER_NUMA 48

#define NVM_FILE_PATH0 "/mnt/pmem/cclbtree/"
#define NVM_FILE_PATH1 "/pmem/cclbtree/"
#define NVM_FILE_SIZE 40ULL * 1024ULL * 1024ULL * 1024ULL
/* */

#include "tools/mempool_numa.h"
inline threadNVMPools the_thread_nvmpools[NUM_NUMA_NODE];
#define the_nvmpool (the_thread_nvmpools[worker_id / NUM_CORE_PER_NUMA].tm_pools[worker_id % NUM_CORE_PER_NUMA])

static int file_exists(const char *filename)
{
	struct stat buffer;
	return stat(filename, &buffer);
}

inline char nvmpool_path0[100], nvmpool_path1[100];

static void openPmemobjPool()
{

	strcpy(nvmpool_path0, NVM_FILE_PATH0);
	strcat(nvmpool_path0, "leafdata");
	strcpy(nvmpool_path1, NVM_FILE_PATH1);
	strcat(nvmpool_path1, "leafdata");

	int sds_write_value = 0;
	pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

	if (num_threads < NUM_CORE_PER_NUMA) // Just allocate memory in node 0.
	{
		the_thread_nvmpools[0].init(num_threads + 1, nvmpool_path0, NVM_FILE_SIZE); // main thread and background thread(our tree) occupied the last pool.
																					// the_thread_nvmpools[1].init(1, nvmpool_path1, 4096);
	}
	else
	{
		the_thread_nvmpools[0].init(NUM_CORE_PER_NUMA + 1, nvmpool_path0, NVM_FILE_SIZE); // main thread and background thread(our tree) occupied the last pool.
		the_thread_nvmpools[1].init(num_threads - NUM_CORE_PER_NUMA + 1, nvmpool_path1, NVM_FILE_SIZE);
	}
}

/******************************for  log*******************************************/
// extend to numa
#include "tools/log_numa.h"

inline nvmLogPool per_numa_log_pool[NUM_NUMA_NODE];

#define the_logpool (per_numa_log_pool[thread_id / NUM_CORE_PER_NUMA].thread_log_pool[thread_id % NUM_CORE_PER_NUMA])
#define logpool_add_log the_logpool.add_log

inline void log_init()
{
	per_numa_log_pool[0].init(NVM_FILE_PATH0);
	per_numa_log_pool[1].init(NVM_FILE_PATH1);
}

static uint64_t total_lnode();

static int get_log_file_cnt()
{
	int total = 0;
	for (int i = 0; i < NUM_NUMA_NODE; i++)
	{
		total += per_numa_log_pool[i].log_file_cnt;
	}
	return total;
}

static uint64_t get_log_totsize()
{
	int totsize = 0;
	for (int i = 0; i < NUM_NUMA_NODE; i++)
	{
		totsize += per_numa_log_pool[i].get_log_totsize();
	}
	return totsize;
}

static uint64_t get_flush_totnum()
{
	int totsize = 0;
	for (int i = 0; i < NUM_NUMA_NODE; i++)
	{
		totsize += per_numa_log_pool[i].get_flush_totnum();
	}
	return totsize;
}

static bool if_log_recycle()
{
	uint64_t tot_size = get_log_totsize();
	uint64_t garbage_size = get_flush_totnum() * sizeof(log_entry_t);

	return (tot_size > total_lnode() * 256 * 0.2) && (garbage_size > tot_size * 0.5);
}

/********************************get pmem space**********************************************/
inline uint64_t freed_nvm_space;
static uint64_t getNVMusage()
{
	uint64_t totsize = 0;
	for (int i = 0; i < NUM_NUMA_NODE; i++)
	{
		totsize += (the_thread_nvmpools[i].print_usage());
	}
	return totsize - freed_nvm_space;
}

/********************************get dram space*********************************************/
inline uint64_t ini_dram_space;

static uint64_t getRSS()
{
	FILE *fstats = fopen("/proc/self/statm", "r");
	// the file contains 7 data:
	// vmsize vmrss shared text lib data dt

	size_t buffsz = 0x1000;
	char buff[buffsz];
	buff[buffsz - 1] = 0;
	fread(buff, 1, buffsz - 1, fstats);
	fclose(fstats);
	const char *pos = buff;

	// get "vmrss"
	while (*pos && *pos == ' ')
		++pos;
	while (*pos && *pos != ' ')
		++pos;
	uint64_t rss = atol(pos);

	// get "shared"
	while (*pos && *pos == ' ')
		++pos;
	while (*pos && *pos != ' ')
		++pos;
	uint64_t shared = atol(pos);
	// ull shared = 0;
	//	return rss*4*1024;
	return (rss - shared) * 4 * 1024; // B
}

static void check_defines()
{

#ifdef INSERT_REPEAT_KEY
	printf("INSERT_REPEAT_KEY\n");
#endif

#ifdef FIXED_BACKGROUND
	printf("FIXED_BACKGROUND\n");
#endif

#ifdef UNIFIED_NODE
	printf("UNIFIED_NODE\n");
#endif

#ifdef eADR_TEST
	printf("eADR_TEST\n");
#endif

#ifdef DRAM_SPACE_TEST
	printf("DRAM_SPACE_TEST\n");
#endif

#ifdef TREE_NO_BUFFER
	printf("TREE_NO_BUFFER\n");
#endif

#ifdef TREE_NO_SELECLOG
	printf("TREE_NO_SELECLOG\n");
#endif

#ifdef DO_WARMUP
	printf("DO_WARMUP\n");
#endif

#ifdef DO_INSERT
	printf("DO_INSERT\n");
#endif

#ifdef DO_UPDATE
	printf("DO_UPDATE\n");
#endif

#ifdef DO_SEARCH
	printf("DO_SEARCH\n");
#endif

#ifdef DO_SCAN
	printf("DO_SCAN\n");
#endif

#ifdef DO_DELETE
	printf("DO_DELETE\n");
#endif
}

/*****************************************************global variable init**********************************/
static void init_global_variable()
{
	check_defines();

#ifdef PIN_CPU
	pin_cpu_core(num_threads); // main thread and gc thread pin to the same cpu core.
#endif

	worker_id = num_threads; // main thread and gc thread use the same pmem pool;
	thread_id = num_threads; // thread id for main thread.

#ifndef FIXED_BACKGROUND
	parallel_merge_worker_num = num_threads; // for dptree
#endif

	for (int i = 0; i < 100; i++)
	{
		count_log_group[i] = 0;
		count_lnode_group[i] = 0;
		elapsed_time_group[i] = 0;

		count_error_insert[i] = 0;
		count_error_update[i] = 0;
		count_error_search[i] = 0;
		count_error_delete[i] = 0;
		count_error_scan[i] = 0;

		count_conflict_in_bnode[i] = 0;

		create_bnode[i] = 0;
		free_bnode[i] = 0;
	}

	log_init();

	ini_dram_space = getRSS();
	printf("dram space after init_global_variable: %fMB\n", ini_dram_space / 1024.0 / 1024);

	freed_nvm_space = 0;
	dram_space = 0;
}

static uint64_t total_error_insert()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_error_insert[i]);
	}
	return sum;
}

static uint64_t total_error_update()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_error_update[i]);
	}
	return sum;
}

static uint64_t total_error_scan()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_error_scan[i]);
	}
	return sum;
}

static uint64_t total_error_search()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_error_search[i]);
	}
	return sum;
}

static uint64_t total_error_delete()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_error_delete[i]);
	}
	return sum;
}

static uint64_t total_error()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_error_insert[i] + count_error_update[i] + count_error_search[i] + count_error_scan[i] + count_error_delete[i]);
	}
	return sum;
}

static uint64_t total_lnode()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_lnode_group[i]);
	}
	return sum;
}

static uint64_t total_log()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_log_group[i]);
	}
	return sum;
}

static uint64_t total_conflict_in_bnode()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (count_conflict_in_bnode[i]);
	}
	return sum;
}

static uint64_t total_free_bnode()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (free_bnode[i]);
	}
	return sum;
}

static uint64_t total_create_bnode()
{
	uint64_t sum = 0;
	for (int i = 0; i < 100; i++)
	{
		sum += (create_bnode[i]);
	}
	return sum;
}