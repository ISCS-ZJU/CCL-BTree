#pragma once

typedef struct log_entry_s log_entry_t;
typedef struct log_chunk_s log_chunk_t;
typedef struct log_s log_t;
typedef struct vlog_s vlog_t;
typedef struct log_group_s log_group_t;
typedef struct vlog_group_s vlog_group_t;
typedef struct free_log_chunks_s free_log_chunks_t;
typedef struct log_file_s log_file_t;

#define LOG_ENTRY_SIZE (sizeof(log_entry_t))

#define LOG_CHUNK_SIZE (4194304ULL) // 4MB

#define LOG_ENTRYS_PER_CHUNK 174762 //(4194304-8)/24

#define LOG_FILE_SIZE 1200 // the number of chunks in a log file

struct log_entry_s
{
    uint64_t key;
    uint64_t value;
    uint64_t timestamp;
};

struct log_chunk_s
{
    log_chunk_t *next;
    log_entry_t log_entries[0];
};

struct log_s
{
    log_chunk_t *head;
};

struct vlog_s
{
    log_chunk_t *now_chunk;
    uint64_t tot_size;
    uint64_t entry_cnt;
};

struct log_group_s
{
    uint8_t alt;
    log_t log[2];
};

struct vlog_group_s
{
    uint8_t alt;
    vlog_t vlog[2];
    uint64_t flushed_count[2];
};

struct free_log_chunks_s
{
    log_chunk_t *head;
    pthread_mutex_t lock;
};

void add_log(uint64_t key, uint64_t value);
void log_vlog_init(log_t &log, vlog_t &vlog, bool is_first);
uint64_t get_log_totsize();
uint64_t get_flush_totnum();

void switch_alt_and_init(int i);
void collect_old_log_to_freelist(int i);
