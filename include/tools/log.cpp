#include "../util.h"

static inline char *log_file_create(uint64_t file_size)
{
    char *tmp;
    size_t mapped_len;
    char str[100];
    char suffix[100];
    int is_pmem;

    strcpy(str, NVM_FILE_PATH0);
    sprintf(suffix, "log_file_%d", log_file_cnt++);
    strcat(str, suffix);

    if ((tmp = (char *)pmem_map_file(str, file_size, PMEM_FILE_CREATE | PMEM_FILE_SPARSE, 0666, &mapped_len, &is_pmem)) == NULL)
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

static inline log_chunk_t *get_log_chunk()
{
    pthread_mutex_lock(&(global_log_chunks.lock));
    if (!global_log_chunks.head->next) // create a new log file.
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

void add_log(uint64_t key, uint64_t value)
{
    uint32_t tid = thread_id;
    // std::cout << "--" << tid << "--" << key << " " << value << std::endl;

    uint8_t alt = vlog_groups[tid]->alt;
    vlog_t *vlog = &(vlog_groups[tid]->vlog[alt]);
    log_chunk_t *tail = vlog->now_chunk;
    log_t *log = &(log_groups[tid]->log[alt]);

    // empty log || current chunk has been run out
    if (vlog->tot_size == 0 || vlog->entry_cnt + 1 == LOG_ENTRYS_PER_CHUNK)
    {
        log_chunk_t *new_chunk = get_log_chunk();
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
    clflush(&(lchunk->log_entries[vlog->entry_cnt]), LOG_ENTRY_SIZE, true);

    vlog->entry_cnt++;
}

void log_vlog_init(log_t &log, vlog_t &vlog, bool is_first)
{
    if (is_first)
    {
        log.head = (log_chunk_t *)pmem_malloc(sizeof(log_chunk_t *));
    }

    log.head->next = NULL;

    vlog.now_chunk = log.head;
    vlog.tot_size = 0;
    vlog.entry_cnt = 0;
}

uint64_t get_log_totsize()
{
    uint64_t tot_size = 0;
    for (int i = 0; i <= num_threads; i++)
    {
        vlog_t *vlog = &(vlog_groups[i]->vlog[vlog_groups[i]->alt]);
        if (vlog->tot_size > 0)
            tot_size += (vlog->tot_size - LOG_CHUNK_SIZE) + vlog->entry_cnt * LOG_ENTRY_SIZE;
    }
    return tot_size;
}

uint64_t get_flush_totnum()
{
    uint64_t tot_num = 0;
    for (int i = 0; i <= num_threads; i++)
    {
        tot_num += vlog_groups[i]->flushed_count[vlog_groups[i]->alt];
    }
    return tot_num;
}

void switch_alt_and_init(int i)
{
    uint8_t x = vlog_groups[i]->alt;
    uint8_t alt = 1 - x;

    vlog_groups[i]->flushed_count[alt] = 0;
    log_vlog_init(log_groups[i]->log[alt], vlog_groups[i]->vlog[alt], false);
    vlog_groups[i]->alt = alt;
    log_groups[i]->alt = alt;
    clflush(log_groups[i], sizeof(log_group_t));
}

void collect_old_log_to_freelist(int i)
{
    uint8_t alt = vlog_groups[i]->alt; 
    uint8_t x = 1 - alt;               
    vlog_t *vlog = &(vlog_groups[i]->vlog[x]);

    if (vlog->tot_size != 0)
    {
        log_t *log = &(log_groups[i]->log[x]);
        log_chunk_t *tail = vlog->now_chunk;

        pthread_mutex_lock(&(global_log_chunks.lock));
        tail->next = global_log_chunks.head->next;
        global_log_chunks.head->next = log->head->next;
        pthread_mutex_unlock(&(global_log_chunks.lock));

        vlog->tot_size = 0;
    }
}