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
#include <climits>

#include "tools/persist.h"
#include "tools/timer.h"
#include "tools/utils.h"
#include "tools/zipfian_generator.h"
#include "tools/scrambled_zipfian_generator.h"
#include "tools/log.h"
#include "tools/nodepref.h"
#include <unistd.h>
#include <sstream>

#define key_type_sob int64_t
#define value_type_sob int64_t

#define SHUFFLE_KEYS

/**********************************DEFINE**************************************/
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

/*****************************************************global variable**********************************/
#define NVM_FILE_PATH0 "/mnt/pmem/cclbtree/"
#define NVM_FILE_SIZE 40ULL * 1024ULL * 1024ULL * 1024ULL

#include "tools/mempool.h"

static int file_exists(const char *filename)
{
	struct stat buffer;
	return stat(filename, &buffer);
}

inline char nvmpool_path[100];
static void openPmemobjPool()
{

	strcpy(nvmpool_path, NVM_FILE_PATH0);
	strcat(nvmpool_path, "leafdata");

	int sds_write_value = 0;
	pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

	the_thread_nvmpools.init(num_threads + 1, nvmpool_path, NVM_FILE_SIZE); // the main thread and the background thread(our tree) occupied the last pool.
}

#define ABORT_INODE 5
#define ABORT_BNODE 6
#define ABORT_LNODE 7

/******************************for  log*******************************************/
static void *pmem_malloc(uint32_t size)
{
	void *ret = malloc(size);
	assert(ret);
	return ret;
}

inline free_log_chunks_t global_log_chunks;
inline vlog_group_t *vlog_groups[100];
inline log_group_t *log_groups[100];
inline uint32_t log_file_cnt = 0;

static void log_init()
{
	global_log_chunks.head = (log_chunk_t *)pmem_malloc(sizeof(log_chunk_t *));
	global_log_chunks.head->next = NULL;
	pthread_mutex_init(&(global_log_chunks.lock), NULL);
	for (int i = 0; i <= num_threads; i++)
	{
		vlog_groups[i] = (vlog_group_t *)malloc(sizeof(vlog_group_t));
		vlog_groups[i]->alt = 0;
		log_groups[i] = (log_group_t *)pmem_malloc(sizeof(log_group_t));
		log_groups[i]->alt = 0;
		for (int j = 0; j < 2; j++)
		{
			log_vlog_init(log_groups[i]->log[j], vlog_groups[i]->vlog[j], true);
			vlog_groups[i]->flushed_count[j] = 0;
		}
		clflush(log_groups[i], sizeof(log_group_t));
	}
}

static uint64_t total_lnode();

static int get_log_file_cnt()
{
	return log_file_cnt;
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
	return (the_thread_nvmpools.print_usage() - freed_nvm_space);
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

	worker_id = num_threads; // main thread will share the pmem pool with child thread 0;
	thread_id = num_threads; // thead id for main thread.

#ifndef FIXED_BACKGROUND
	parallel_merge_worker_num = num_threads;
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

	log_init(); ////////////////

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
