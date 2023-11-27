#include "wrapper.h"
#include "util.h"

using namespace std;

void clear_cache()
{
    // Remove cache
    uint64_t size = 256 * 1024 * 1024;
    char *garbage = new char[size];
    for (uint64_t i = 0; i < size; ++i)
        garbage[i] = i;
    for (uint64_t i = 100; i < size; ++i)
        garbage[i] += garbage[i - 100];
    delete[] garbage;
}

int main(int argc, char **argv)
{
    //***************************init op*******************************//
    // generate keys
    if (argc != 4)
    {
        fprintf(stderr, "The parameters (num_keys and num_threads and scan_size) are required\n");
        return 0;
    }

    num_keys = atoi(argv[1]);
    printf("n=%lu\n", num_keys);
    num_threads = atoi(argv[2]);
    mscan_size = atoi(argv[3]);
    mmax_length_for_scan = mscan_size + 64;
    printf("mscan_size=%d  mmax_length_for_scan=%d\n", mscan_size, mmax_length_for_scan);

    init_global_variable();

    key_type_sob *keys = (key_type_sob *)malloc(num_keys * sizeof(key_type_sob));
    assert(keys);
    std::random_device rd;
    std::mt19937_64 eng(rd());

    // std::uniform_int_distribution<key_type_sob> uniform_dist(1, num_keys);
    std::uniform_int_distribution<key_type_sob> uniform_dist;
#ifndef ZIPFIAN
    for (uint64_t i = 0; i < num_keys;)
    {
        key_type_sob x = uniform_dist(eng);
        if (x > 0 && x < INT64_MAX - 1)
        {
            keys[i++] = x;
            // keys[i++] = i + 1;
        }
        else
        {
            continue;
        }
    }
#else
    ZipfianGenerator zf(num_keys);
    printf("zipfian distribution\n");
    for (uint64_t i = 0; i < num_keys; i++)
    {
        keys[i] = zf.Next() + 1;
    }
#endif

    printf("after key init: dram space (RSS) = %fMB\n", (getRSS() - ini_dram_space) / 1024.0 / 1024);

    // std::shuffle(keys, keys + num_keys, eng);
    uint64_t time_start;

    /************************************ global variable*************************************/

    openPmemobjPool();

    tree_init();

    printf("after tree_init() : dram space (RSS) = %fMB\n", (getRSS() - ini_dram_space) / 1024.0 / 1024);

    //***************************multi thread init**********************//
    std::vector<std::future<void>> futures(num_threads);
    uint64_t data_per_thread = (num_keys / 2) / num_threads;

    //***************************warm up**********************//
#ifdef DO_WARMUP
    time_start = NowNanos();
    for (uint64_t tid = 0; tid < num_threads; tid++)
    {

        uint64_t from = data_per_thread * tid;
        uint64_t to = (tid == num_threads - 1) ? num_keys / 2 : from + data_per_thread;
        auto f = async(
            launch::async,
            [&](uint64_t from, uint64_t to, uint64_t tid)
            {
#ifdef PIN_CPU
                pin_cpu_core(tid);
#endif

                worker_id = tid;
                thread_id = tid;

                for (uint64_t i = from; i < to; ++i)
                {
                    tree_insert(keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
        {
            f.get();
        }
    printf("%d threads warm up time cost is %llu ns. error_count = %lld\n", num_threads, ElapsedNanos(time_start), total_error_insert());
    // CCL-BTree needs a long time to warm up because of the pre-touching of NVM log files.

#ifdef DPTREE
    printf("wait for background..\n");
    while (bt->is_merging())
        ;
#endif

    printf("dram space after warmup: %fMB\n", getRSS() / 1024.0 / 1024);
#endif // DO_WARMUP

    //***************************insert op*******************************//
#ifdef DO_INSERT

#ifndef MIXED_WORKLOAD
    clear_cache();
    futures.clear();

    time_start = NowNanos();

    for (uint64_t tid = 0; tid < num_threads; tid++)
    {
        uint64_t from = data_per_thread * tid + num_keys / 2;
        uint64_t to = (tid == num_threads - 1) ? num_keys : from + data_per_thread;

        auto f = async(
            launch::async,
            [&](uint64_t from, uint64_t to, uint64_t tid)
            {
#ifdef PIN_CPU
                pin_cpu_core(tid);
#endif

                worker_id = tid;
                thread_id = tid;

                for (uint64_t i = from; i < to; ++i)
                {
                    tree_insert(keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }

    for (auto &&f : futures)
        if (f.valid())
            f.get();
    printf("%d threads insert time cost is %llu ns. error_count = %lld\n", num_threads, ElapsedNanos(time_start), total_error_insert());

#ifdef DPTREE
    printf("wait for background..\n");
    while (bt->is_merging())
        ;
#endif

    printf("dram space after insert: %fMB\n", getRSS() / 1024.0 / 1024);

#endif // DO_INSERT

    //***************************update op*******************************//
#ifdef DO_UPDATE

#ifdef SHUFFLE_KEYS
    std::shuffle(keys, keys + num_keys, eng);
#endif

    clear_cache();
    futures.clear();
    time_start = NowNanos();

    for (uint64_t tid = 0; tid < num_threads; tid++)
    {
        uint64_t from = data_per_thread * tid + num_keys / 2;
        uint64_t to = (tid == num_threads - 1) ? num_keys : from + data_per_thread;

        auto f = async(
            launch::async,
            [&](uint64_t from, uint64_t to, uint64_t tid)
            {
#ifdef PIN_CPU
                pin_cpu_core(tid);
#endif

                worker_id = tid;
                thread_id = tid;

                for (uint64_t i = from; i < to; ++i)
                {
                    tree_update(keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    printf("%d threads update time cost is %llu ns. error_count = %lld\n", num_threads, ElapsedNanos(time_start), total_error_update());

#endif // end update

#ifdef DPTREE
    printf("wait for background..\n");
    while (bt->is_merging())
        ;
#endif
#endif // DO_UPDATE

        //***************************search op*******************************//
#ifdef DO_SEARCH

#ifdef SHUFFLE_KEYS
    std::shuffle(keys, keys + num_keys, eng);
#endif

    clear_cache();
    futures.clear();
    time_start = NowNanos();

    for (uint64_t tid = 0; tid < num_threads; tid++)
    {
        uint64_t from = data_per_thread * tid + num_keys / 2;
        uint64_t to = (tid == num_threads - 1) ? num_keys : from + data_per_thread;

        auto f = std::async(
            std::launch::async,
            [&](uint64_t from, uint64_t to, uint64_t tid)
            {
#ifdef PIN_CPU
                pin_cpu_core(tid);
#endif
                thread_id = tid;
                for (uint64_t i = from; i < to; ++i)
                {
                    tree_search(keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    printf("%d threads search time cost is %llu ns. error_count = %lld\n", num_threads, ElapsedNanos(time_start), total_error_search());

#ifdef DPTREE
    printf("wait for background..\n");
    while (bt->is_merging())
        ;
#endif

#endif // DO_SEARCH

        //***************************scan op*******************************//
#ifdef DO_SCAN

#ifdef SHUFFLE_KEYS
    std::shuffle(keys, keys + num_keys, eng);
#endif

    clear_cache();
    futures.clear();
    time_start = NowNanos();

    for (uint64_t tid = 0; tid < num_threads; tid++)
    {
        uint64_t from = data_per_thread * tid + num_keys / 2;
        uint64_t to = (tid == num_threads - 1) ? num_keys : from + data_per_thread;

        auto f = std::async(
            std::launch::async,
            [&](uint64_t from, uint64_t to, uint64_t tid)
            {
#ifdef PIN_CPU
                pin_cpu_core(tid);
#endif
                thread_id = tid;

                std::vector<value_type_sob> buf;
                buf.reserve(mmax_length_for_scan);
                for (uint64_t i = from; i < to; ++i)
                {

                    tree_scan(keys[i], mscan_size, buf);
                    buf.clear();
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    printf("%d threads scan time cost is %llu ns. error_count = %lld\n", num_threads, ElapsedNanos(time_start), total_error_scan());

#ifdef DPTREE
    printf("wait for background..\n");
    while (bt->is_merging())
        ;
#endif

#endif // DO_SCAN

        //***************************delete op*******************************//
#ifdef DO_DELETE

#ifdef SHUFFLE_KEYS
    std::shuffle(keys, keys + num_keys, eng);
#endif

    clear_cache();
    futures.clear();
    time_start = NowNanos();

    for (uint64_t tid = 0; tid < num_threads; tid++)
    {
        uint64_t from = data_per_thread * tid + num_keys / 2;
        uint64_t to = (tid == num_threads - 1) ? num_keys : from + data_per_thread;
        // printf("thread %d , from = %d ,to = %d\n", tid, from, to);
        auto f = async(
            launch::async,
            [&](uint64_t from, uint64_t to, uint64_t tid)
            {
#ifdef PIN_CPU
                pin_cpu_core(tid);
#endif

                worker_id = tid;
                thread_id = tid;

                for (uint64_t i = from; i < to; ++i)
                {
                    tree_delete(keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    printf("%d threads delete time cost is %llu ns. error_count = %lld\n", num_threads, ElapsedNanos(time_start), total_error_delete());

#ifdef DPTREE
    printf("wait for background..\n");
    while (bt->is_merging())
        ;
#endif

#endif // DO_DELETE

    // memory overhead
    printf("nvm space = %fMB\n", getNVMusage() / 1024.0 / 1024);
    printf("dram space (RSS) = %fMB\n", (getRSS() - ini_dram_space) / 1024.0 / 1024);

#ifdef DRAM_SPACE_TEST
    printf("dram space (non_lnode_space) = %fMB\n", dram_space / 1024.0 / 1024);
#endif

#if defined(CCLBTREE_LB) || defined(CCLBTREE_FF)
    printf("log_totsize = %fMB\n", get_log_totsize() / 1024.0 / 1024);
#endif

#ifdef PACTREE
    tree_get_memory_footprint();
#endif

    // background threads
#if defined(CCLBTREE_LB) || defined(CCLBTREE_FF)
    signal_run_bgthread = false;
    bg_thread.get();
#endif

#ifdef DPTREE
    tree_end();
#endif

    free(keys);
    return 0;
}
