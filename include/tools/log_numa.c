#include "../util.h"

static inline void *pmem_malloc(uint32_t size)
{
    void *ret = malloc(size);
    assert(ret);
    return ret;
}

static inline void log_vlog_init(log_t &log, vlog_t &vlog, bool is_first)
{
    if (is_first) //
    {
        log.head = (log_chunk_t *)pmem_malloc(sizeof(log_chunk_t *));
    }

    log.head->next = NULL;

    vlog.now_chunk = log.head;
    vlog.tot_size = 0;
    vlog.entry_cnt = 0;
}

void threadLogPool::init()
{
    vlog_groups = (vlog_group_t *)malloc(sizeof(vlog_group_t));
    vlog_groups->alt = 0;
    log_groups = (log_group_t *)pmem_malloc(sizeof(log_group_t));
    log_groups->alt = 0;
    for (int j = 0; j < 2; j++)
    {
        log_vlog_init(log_groups->log[j], vlog_groups->vlog[j], true);
        vlog_groups->flushed_count[j] = 0;
    }
    clflush(log_groups, sizeof(log_group_t));
}

void threadLogPool::add_log(uint64_t key, uint64_t value)
{
    // std::cout << "--" << tid << "--" << key << " " << value << std::endl;

    uint8_t alt = vlog_groups->alt;
    vlog_t *vlog = &(vlog_groups->vlog[alt]);
    log_chunk_t *tail = vlog->now_chunk;
    log_t *log = &(log_groups->log[alt]);

    // empty log || the current chunk has been run out
    if (vlog->tot_size == 0 || vlog->entry_cnt + 1 == LOG_ENTRYS_PER_CHUNK)
    {
        log_chunk_t *new_chunk = per_numa_log_pool[thread_id / NUM_CORE_PER_NUMA].get_log_chunk();
        tail->next = new_chunk;
        // new_chunk->next = NULL;
        //  new_chunk->next = log->head->next;
        //  log->head->next = new_chunk;
        vlog->now_chunk = new_chunk;
        vlog->tot_size += LOG_CHUNK_SIZE;
        vlog->entry_cnt = 0;
    }

    log_chunk_t *lchunk = vlog->now_chunk;
    assert(vlog->entry_cnt < LOG_ENTRYS_PER_CHUNK);
    lchunk->log_entries[vlog->entry_cnt].key = key;
    lchunk->log_entries[vlog->entry_cnt].value = value;
    lchunk->log_entries[vlog->entry_cnt].timestamp = _rdtsc();
    //  = {key, value, _rdtsc()};
    clflush(&(lchunk->log_entries[vlog->entry_cnt]), LOG_ENTRY_SIZE, true);

    vlog->entry_cnt++;
}

void threadLogPool::switch_alt_and_init()
{
    uint8_t x = vlog_groups->alt;
    uint8_t alt = 1 - x;
    vlog_groups->flushed_count[alt] = 0;
    log_vlog_init(log_groups->log[alt], vlog_groups->vlog[alt], false);
    vlog_groups->alt = alt;
    log_groups->alt = alt;
    clflush(log_groups, sizeof(log_group_t));
}

void nvmLogPool::init(const char *path)
{
    strcpy(pmempath, path);
    strcat(pmempath,"log_file");
    global_log_chunks.head = (log_chunk_t *)pmem_malloc(sizeof(log_chunk_t *));
    global_log_chunks.head->next = NULL;
    pthread_mutex_init(&(global_log_chunks.lock), NULL);
    for (int i = 0; i <= num_threads; i++)
    {
        thread_log_pool[i].init();
    }
}

void nvmLogPool::collect_old_log_to_freelist()
{

    for (int i = 0; i <= num_threads; i++)
    {
        uint8_t alt = thread_log_pool[i].vlog_groups->alt; /////////////
        uint8_t x = 1 - alt;                               // old log
        vlog_t *vlog = &(thread_log_pool[i].vlog_groups->vlog[x]);

        if (vlog->tot_size != 0)
        {
            log_t *log = &(thread_log_pool[i].log_groups->log[x]);
            log_chunk_t *tail = vlog->now_chunk;

            pthread_mutex_lock(&(global_log_chunks.lock));
            tail->next = global_log_chunks.head->next;
            global_log_chunks.head->next = log->head->next;
            pthread_mutex_unlock(&(global_log_chunks.lock));

            vlog->tot_size = 0;
        }
    }
}

char *nvmLogPool::log_file_create(uint64_t file_size)
{
    char *tmp;
    size_t mapped_len;
    char tmppath[100];
    char str[100];
    int is_pmem;

    sprintf(tmppath, pmempath);
    sprintf(str, "_%d", log_file_cnt++);
    strcat(tmppath, str);

    std::cout << tmppath << " " << file_size << " " << thread_id << std::endl;

    if ((tmp = (char *)pmem_map_file(tmppath, file_size, PMEM_FILE_CREATE | PMEM_FILE_SPARSE, 0666, &mapped_len, &is_pmem)) == NULL)
    {
        printf("map file fail!1\n %d\n", errno);
        exit(1);
    }
    if (!is_pmem)
    {
        printf("is not nvm!\n");
        exit(1);
    }

    assert(tmp);
    memset(tmp, 0, file_size);
    printf("..log_file_create end.\n");
    return tmp;
}

log_chunk_t *nvmLogPool::get_log_chunk()
{

    pthread_mutex_lock(&(global_log_chunks.lock));
    if (!global_log_chunks.head->next)
    {
        log_chunk_t *addr = (log_chunk_t *)log_file_create(LOG_CHUNK_SIZE * LOG_FILE_SIZE);
        log_chunk_t *pre = addr;
        pre->next = global_log_chunks.head->next;
        addr = (log_chunk_t *)((uintptr_t)addr + LOG_CHUNK_SIZE);
        for (int i = 0; i < LOG_FILE_SIZE - 1; i++)
        {
            addr->next = pre;
            pre = addr;
            addr = (log_chunk_t *)((uintptr_t)addr + LOG_CHUNK_SIZE);
        }
        global_log_chunks.head->next = pre;
        assert(global_log_chunks.head->next);
    }

    log_chunk_t *ret = global_log_chunks.head->next;
    assert(ret);
    global_log_chunks.head->next = ret->next;
    pthread_mutex_unlock(&(global_log_chunks.lock));
    ret->next = NULL;
    return ret;
}

uint64_t nvmLogPool::get_log_totsize()
{
    uint64_t tot_size = 0;
    for (int i = 0; i <= num_threads; i++)
    {
        vlog_t *vlog = &(thread_log_pool[i].vlog_groups->vlog[thread_log_pool[i].vlog_groups->alt]);
        if (vlog->tot_size > 0)
            tot_size += (vlog->tot_size - LOG_CHUNK_SIZE) + vlog->entry_cnt * LOG_ENTRY_SIZE;
    }
    return tot_size;
}

uint64_t nvmLogPool::get_flush_totnum()
{
    uint64_t tot_num = 0;
    for (int i = 0; i <= num_threads; i++)
    {
        tot_num += thread_log_pool[i].vlog_groups->flushed_count[thread_log_pool[i].vlog_groups->alt];
    }
    return tot_num;
}
