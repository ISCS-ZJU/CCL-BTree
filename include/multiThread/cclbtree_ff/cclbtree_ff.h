#pragma once
#include "util.h"

#ifndef UNIFIED_NODE
#define NON_LEAF_KEY_NUM (NONLEAF_SIZE / (KEY_SIZE + POINTER_SIZE) - 1)
#else
#define NON_LEAF_KEY_NUM 14 // node size
#endif
// #define NON_LEAF_KEY_NUM (64) //node size
#define LEAF_KEY_NUM (14)
#define CACHE_KEY_NUM (2)

#define bitScan(x) __builtin_ffs(x)
#define countBit(x) __builtin_popcount(x)

#define MUTEX

volatile bool signal_do_recycle;

void sfence()
{
    _mm_sfence();
    // _mm_mfence();
}

typedef struct leaf_entry
{
    key_type_sob k;
    char *v;
} leaf_entry;

#define IS_LOCKED(x) ((x) % 2 != 0)
/**
 *  bottom node.   //a transfer station for leaf node
 *
 */
typedef union bnodeMeta
{
    uint64_t padding8B;
    struct
    {
        uint64_t ptr : 48;      // pointer to leaf node
        uint64_t version : 7;   // version lock to achieve a lock-free search
        uint64_t epoch_num : 6; // for GC
        uint64_t counter : 3;   // the number of KVs that are not flushed to leaf nodes
    } v;
} bnodeMeta;

typedef struct bnode
{
    bnodeMeta meta;
    leaf_entry cache[CACHE_KEY_NUM];
} bnode; // bnode

/**
 * leafnode: leaf node
 *
 *
 */
typedef struct lnodeMeta
{
    uint64_t bitmap : 14;
    uint64_t next : 48;
    uint64_t timestamp;
    unsigned char fgpt[LEAF_KEY_NUM]; /* fingerprints */
    unsigned char padding[2];
} lnodeMeta;

class lnode
{
public:
    lnodeMeta meta;
    leaf_entry ent[LEAF_KEY_NUM];

public:
    key_type_sob &k(int idx) { return ent[idx].k; }
    char *&ch(int idx) { return ent[idx].v; }

    int num() { return countBit(meta.bitmap); }

    bool isFull(void) { return (meta.bitmap == 0x3fff); }
    bool isAlmostFull(int a) { return (countBit(meta.bitmap) + a) > LEAF_KEY_NUM; }

    void setMeta(lnodeMeta *m)
    {
        memcpy(&meta, m, sizeof(lnodeMeta)); // should be 2 memcpy(addr , m , 8)
    }
}; // leafnode

#ifndef UNIFIED_NODE
#define PAGESIZE 512 // ori 512
#else
#define PAGESIZE 256 // node size
#endif

#define IS_FORWARD(c) (c % 2 == 0)

using entry_key_t = int64_t;

inline bool get_lock_bnode(bnode *bn, uint8_t op_type, uint8_t &version)
{
    if (bn == NULL)
        return false;

    bnodeMeta old_meta = bn->meta;
    if (IS_LOCKED(old_meta.v.version))
    {
        return false;
    }

    if (op_type == 0) // insert
    {
        bnodeMeta new_meta = old_meta;
        new_meta.v.version++;
        bool ret = __sync_bool_compare_and_swap((uint64_t *)bn, old_meta.padding8B, new_meta.padding8B);
        _mm_mfence();
        return ret;
    }
    else // search
    {
        version = old_meta.v.version;
        _mm_mfence();
        return true;
    }
}

inline bool reset_lock_bnode(bnode *bn, uint8_t op_type)
{

    if (op_type == 0)
    { // insert
        if (bn == NULL)
        {
            assert("error! bn==NULL !\n");
        }
        else if (!IS_LOCKED(bn->meta.v.version))
        {
            assert("error! bn->lock == 0!\n");
        }
        else
        {
            _mm_mfence();
            bn->meta.v.version++;
        }
        return true;
    }
    else
    { // search, just return
        return true;
    }
}

// pos[] will contain sorted positions
inline void qsortBleaf(lnode *p, int start, int end, int pos[])
{
    if (start >= end)
        return;

    int pos_start = pos[start];
    key_type_sob key = p->k(pos_start); // pivot
    int l, r;

    l = start;
    r = end;
    while (l < r)
    {
        while ((l < r) && (p->k(pos[r]) > key))
            r--;
        if (l < r)
        {
            pos[l] = pos[r];
            l++;
        }
        while ((l < r) && (p->k(pos[l]) <= key))
            l++;
        if (l < r)
        {
            pos[r] = pos[l];
            r--;
        }
    }
    pos[l] = pos_start;
    qsortBleaf(p, start, l - 1, pos);
    qsortBleaf(p, l + 1, end, pos);
}

class page;

class btree
{
private:
    int height;
    char *root;

public:
    int epoch_num;
    lnode *first_lnode;
    page *first_inode;

    btree();
    ~btree();
    void setNewRoot(char *);
    void getNumberOfNodes();
    void btree_insert_pred(entry_key_t, char *, char **pred, bool *);
    void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
    void btree_delete(entry_key_t);
    char *btree_search(entry_key_t);
    char *btree_search_pred(entry_key_t, bool *f, char **, bool);
    int btree_search_range(entry_key_t min_key, uint64_t len, std::vector<value_type_sob> &buf);

    void printAll();
    void printinfo_leaf();
    bool insert(entry_key_t, char *, bool update); // Insert
    bool insert_into_leaf(bnode *bn, page *parent, entry_key_t key, char *val, bool update);
    void remove(entry_key_t);                                                  // Remove
    char *search(entry_key_t);                                                 // Search
    int scan(entry_key_t key, uint64_t len, std::vector<value_type_sob> &buf); // Scan

    bnode *get_the_target_bnode(entry_key_t key, uint8_t op_type, bnode **pred, page **inode);
    void recycle_bottom();
    void recycle_bottom_naive();
    void clean_bottom();
    friend class page;
};

class header
{
private:
#ifdef MUTEX
    std::mutex *mtx; // 8 bytes
#else
    pthread_rwlock_t *rwlock; // 8 bytes
#endif
    uint64_t leftmost_ptr : 48; // 8 bytes
    uint8_t switch_counter : 15;
    uint8_t is_deleted : 1;
    uint64_t sibling_ptr : 48; // 8 bytes
    uint8_t level : 8;         //
    int16_t last_index : 8;    //
    entry_key_t minkey;        // 8 bytes

    friend class page;
    friend class btree;

public:
    header()
    {
#ifdef MUTEX
        mtx = new std::mutex();
#else
        rwlock = new pthread_rwlock_t;

        pthread_rwlockattr_t attr;
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
        pthread_rwlock_init(rwlock, &attr);
#endif
        leftmost_ptr = NULL;
        sibling_ptr = NULL;
        // pred_ptr = NULL;
        switch_counter = 0;
        last_index = -1;
        is_deleted = false;

        minkey = 0;
    }

    ~header()
    {
#ifdef MUTEX delete mtx;
#else
        pthread_rwlock_destroy(rwlock);
        delete rwlock;
#endif
    }
};

class entry
{
private:
    entry_key_t key; // 8 bytes
    char *ptr;       // 8 bytes

public:
    entry()
    {
        key = LONG_MAX;
        ptr = NULL;
    }

    friend class page;
    friend class btree;
};

const int cardinality = (PAGESIZE - sizeof(header)) / sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class page
{
private:
    header hdr;                 // header in persistent memory, 16 bytes
    entry records[cardinality]; // slots in persistent memory, 16 bytes * n

public:
    friend class btree;

    page(uint32_t level = 0)
    {
        hdr.level = level;
        records[0].ptr = NULL;
    }

    // this is called when tree grows
    page(page *left, entry_key_t key, page *right, uint32_t level = 0)
    {
        hdr.leftmost_ptr = (uint64_t)left;
        hdr.level = level;
        records[0].key = key;
        records[0].ptr = (char *)right;
        records[1].ptr = NULL;

        hdr.last_index = 0;
    }

    void *operator new(size_t size)
    {
#ifdef DRAM_SPACE_TEST
        __sync_fetch_and_add(&dram_space, size);
#endif

        void *ret;
        posix_memalign(&ret, 64, size);
        return ret;
    }

    inline int count()
    {
        uint8_t previous_switch_counter;
        int count = 0;
        do
        {
            previous_switch_counter = hdr.switch_counter;
            count = hdr.last_index + 1;

            while (count >= 0 && records[count].ptr != NULL)
            {
                if (IS_FORWARD(previous_switch_counter))
                    ++count;
                else
                    --count;
            }

            if (count < 0)
            {
                count = 0;
                while (records[count].ptr != NULL)
                {
                    ++count;
                }
            }

        } while (previous_switch_counter != hdr.switch_counter);

        return count;
    }
#if 1
    inline bool remove_key(entry_key_t key)
    {
        // Set the switch_counter
        if (IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        bool shift = false;
        int i;
        for (i = 0; records[i].ptr != NULL; ++i)
        {
            if (!shift && records[i].key == key)
            {
                records[i].ptr = (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
                shift = true;
            }

            if (shift)
            {
                records[i].key = records[i + 1].key;
                records[i].ptr = records[i + 1].ptr;
            }
        }

        if (shift)
        {
            --hdr.last_index;
        }
        return shift;
    }
#endif

    bool remove(btree *bt, entry_key_t key, bool with_lock = true)
    {
#ifdef MUTEX
        hdr.mtx->lock();
#else
        pthread_rwlock_wrlock(hdr.rwlock);
#endif
        // If this node has a sibling node,
        if (hdr.sibling_ptr && (hdr.sibling_ptr != NULL))
        {
            // Compare this key with the first key of the sibling

            if (key >= ((page *)hdr.sibling_ptr)->hdr.minkey)
            {
                if (with_lock)
                {
#ifdef MUTEX
                    hdr.mtx->unlock(); // Unlock the write lock
#else
                    pthread_rwlock_unlock(hdr.rwlock);
#endif
                }
                return ((page *)hdr.sibling_ptr)->remove(bt, key, true);
            }
        }

        bool ret = remove_key(key);

#ifdef MUTEX
        hdr.mtx->unlock();
#else
        pthread_rwlock_unlock(hdr.rwlock);
#endif
        return ret;
    }

    inline void insert_key(entry_key_t key, char *ptr, int *num_entries,
                           bool update_last_index = true)
    {
        // update switch_counter
        if (!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        // FAST
        if (*num_entries == 0)
        { // this page is empty
            entry *new_entry = (entry *)&records[0];
            entry *array_end = (entry *)&records[1];
            new_entry->key = (entry_key_t)key;
            new_entry->ptr = (char *)ptr;

            array_end->ptr = (char *)NULL;
        }
        else
        {
            int i = *num_entries - 1, inserted = 0;
            records[*num_entries + 1].ptr = records[*num_entries].ptr;

            // FAST
            for (i = *num_entries - 1; i >= 0; i--)
            {
                if (key < records[i].key)
                {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key = records[i].key;
                }
                else
                {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key = key;
                    records[i + 1].ptr = ptr;
                    inserted = 1;
                    break;
                }
            }
            if (inserted == 0)
            {
                records[0].ptr = (char *)hdr.leftmost_ptr;
                records[0].key = key;
                records[0].ptr = ptr;
            }
        }

        if (update_last_index)
        {
            hdr.last_index = *num_entries;
        }
        ++(*num_entries);
    }

    // Insert a new key - FAST and FAIR
    page *store(btree *bt, entry_key_t key, char *right,
                bool with_lock, page *invalid_sibling = NULL)
    {
        if (with_lock)
        {
#ifdef MUTEX
            hdr.mtx->lock(); // Lock the write lock
#else
            pthread_rwlock_wrlock(hdr.rwlock);
#endif
        }
        if (hdr.is_deleted)
        {
            if (with_lock)
            {
#ifdef MUTEX
                hdr.mtx->unlock();
#else
                pthread_rwlock_unlock(hdr.rwlock);
#endif
            }

            return NULL;
        }

        register int num_entries = count();

        // the key can not exist
        //  for (int i = 0; i < num_entries; i++)
        //      if (key == records[i].key)
        //      {
        //          records[i].ptr = right;
        //          if (with_lock)
        //              hdr.mtx->unlock();
        //          return this;
        //      }

        // If this node has a sibling node,
        if (hdr.sibling_ptr && ((page *)hdr.sibling_ptr != invalid_sibling))
        {
            // Compare this key with the first key of the sibling
            // if (key > hdr.sibling_ptr->records[0].key)
            // {
            //     if (with_lock)
            //     {
            //         // hdr.mtx->unlock(); // Unlock the write lock
            //         pthread_rwlock_unlock(hdr.rwlock);
            //     }
            //     return hdr.sibling_ptr->store(bt, key, right, with_lock, invalid_sibling);
            // }

            if (key >= ((page *)hdr.sibling_ptr)->hdr.minkey)
            {
                if (with_lock)
                {
#ifdef MUTEX
                    hdr.mtx->unlock(); // Unlock the write lock
#else
                    pthread_rwlock_unlock(hdr.rwlock);
#endif
                }
                return ((page *)hdr.sibling_ptr)->store(bt, key, right, with_lock, invalid_sibling);
            }
        }

        // FAST
        if (num_entries < cardinality - 1)
        {
            insert_key(key, right, &num_entries);

            if (with_lock)
            {
#ifdef MUTEX
                hdr.mtx->unlock(); // Unlock the write lock
#else
                pthread_rwlock_unlock(hdr.rwlock);
#endif
            }

            return this;
        }
        else
        { // FAIR
            // overflow
            // create a new node
            page *sibling = new page(hdr.level);
            register int m = (int)ceil(num_entries / 2);
            entry_key_t split_key = records[m].key;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            if (hdr.leftmost_ptr == NULL)
            { // leaf node
                assert(false && "error: can not be a leaf node!\n");
                for (int i = m; i < num_entries; ++i)
                {
                    sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt);
                }
            }
            else
            { // internal node
                for (int i = m + 1; i < num_entries; ++i)
                {
                    sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt);
                }
                sibling->hdr.leftmost_ptr = (uint64_t)records[m].ptr;
                sibling->hdr.minkey = records[m].key;
            }

            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            // sibling->hdr.pred_ptr = this;
            // if (sibling->hdr.sibling_ptr != NULL)
            //     sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
            hdr.sibling_ptr = (uint64_t)sibling;

            // set to NULL
            if (IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter += 2;
            else
                ++hdr.switch_counter;
            records[m].ptr = NULL;
            hdr.last_index = m - 1;
            num_entries = hdr.last_index + 1;

            page *ret;

            // insert the key
            if (key < split_key)
            {
                insert_key(key, right, &num_entries);
                ret = this;
            }
            else
            {
                sibling->insert_key(key, right, &sibling_cnt);
                ret = sibling;
            }

            // Set a new root or insert the split key to the parent
            if (bt->root == (char *)this)
            { // only one node can update the root ptr
                page *new_root = new page((page *)this, split_key, sibling,
                                          hdr.level + 1);
                bt->setNewRoot((char *)new_root);

                if (with_lock)
                {
#ifdef MUTEX
                    hdr.mtx->unlock(); // Unlock the write lock
#else
                    pthread_rwlock_unlock(hdr.rwlock);
#endif
                }
            }
            else
            {
                if (with_lock)
                {
#ifdef MUTEX
                    hdr.mtx->unlock(); // Unlock the write lock
#else
                    pthread_rwlock_unlock(hdr.rwlock);
#endif
                }
                bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                          hdr.level + 1);
            }

            return ret;
        }
    }

    char *
    linear_search(entry_key_t key)
    {
        int i = 1;
        uint8_t previous_switch_counter;
        char *ret = NULL;
        char *t;
        entry_key_t k;
        { // internal node
            do
            {
                previous_switch_counter = hdr.switch_counter;
                ret = NULL;

                if (IS_FORWARD(previous_switch_counter))
                {
                    if (key < (k = records[0].key))
                    {
                        if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr)
                        {
                            ret = t;
                            continue;
                        }
                    }

                    for (i = 1; records[i].ptr != NULL; ++i)
                    {
                        if (key < (k = records[i].key))
                        {
                            if ((t = records[i - 1].ptr) != records[i].ptr)
                            {
                                ret = t;
                                break;
                            }
                        }
                    }

                    if (!ret)
                    {
                        ret = records[i - 1].ptr;
                        continue;
                    }
                }
                else
                { // search from right to left
                    for (i = count() - 1; i >= 0; --i)
                    {
                        if (key >= (k = records[i].key))
                        {
                            if (i == 0)
                            {
                                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr))
                                {
                                    ret = t;
                                    break;
                                }
                            }
                            else
                            {
                                if (records[i - 1].ptr != (t = records[i].ptr))
                                {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
            } while (hdr.switch_counter != previous_switch_counter);

            if ((t = (char *)hdr.sibling_ptr) != NULL)
            {
                if (key >= ((page *)t)->records[0].key)
                    return t;
            }

            if (ret)
            {
                return ret;
            }
            else
                return (char *)hdr.leftmost_ptr;
        }

        return NULL;
    }

    // op_type: 0 indicates insert operations and 1 indicates search operations.
    char *linear_search_last_level_pred(entry_key_t key, uint8_t op_type, uint8_t &version, bnode **pred)
    {
        int i = 1;
        uint8_t previous_switch_counter;
        char *ret = NULL;
        char *t;
        entry_key_t k;

        int index = -1;

    retry:

        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter))
        {
            if (key < (k = records[0].key)) // Special case, leftmost
            {
                if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr)
                {
                    ret = t;
                    index = -1; // leftmost

                    goto find;
                }
            }

            for (i = 1; records[i].ptr != NULL; ++i)
            {
                if (key < (k = records[i].key))
                {
                    if ((t = records[i - 1].ptr) != records[i].ptr)
                    {
                        ret = t;
                        index = i - 1;
                        break;
                    }
                }
            }

            // Special case, rightmost
            if (!ret)
            {
                ret = records[i - 1].ptr;
                index = i - 1;

                goto find;
            }
        }
        else
        { // search from right to left
            for (i = count() - 1; i >= 0; --i)
            {
                if (key >= (k = records[i].key))
                {
                    if (i == 0)
                    {
                        if ((char *)hdr.leftmost_ptr != (t = records[i].ptr))
                        {
                            ret = t;
                            index = i;

                            break;
                        }
                    }
                    else
                    {
                        if (records[i - 1].ptr != (t = records[i].ptr))
                        {
                            ret = t;
                            index = i;

                            break;
                        }
                    }
                }
            }

            if (!ret)
            {
                ret = (char *)hdr.leftmost_ptr;
                index = -1;
            }
        }

    find:
        if (!get_lock_bnode((bnode *)ret, op_type, version))
        {
            goto retry;
        }

        _mm_mfence();

        if (hdr.switch_counter != previous_switch_counter || (index < count() - 1 && key >= records[index + 1].key))
        {
            reset_lock_bnode((bnode *)ret, op_type);
            goto retry;
        }

        // If this node has a sibling node,
        if (hdr.sibling_ptr)
        {
            // Compare this key with the first key of the sibling

            if (key >= ((page *)hdr.sibling_ptr)->hdr.minkey)
            {
                reset_lock_bnode((bnode *)ret, op_type);
                return ((page *)hdr.sibling_ptr)->linear_search_last_level_pred(key, op_type, version, pred);
            }
        }

        return ret;
    }

    // print a node
    void
    print()
    {
        if (hdr.leftmost_ptr == NULL)
            printf("[%d] leaf %x \n", this->hdr.level, this);
        else
            printf("[%d] internal %x \n", this->hdr.level, this);
        printf("last_index: %d\n", hdr.last_index);
        printf("switch_counter: %d\n", hdr.switch_counter);
        printf("search direction: ");
        if (IS_FORWARD(hdr.switch_counter))
            printf("->\n");
        else
            printf("<-\n");

        if (hdr.leftmost_ptr != NULL)
            printf("%x ", hdr.leftmost_ptr);

        for (int i = 0; records[i].ptr != NULL; ++i)
            printf("%ld,%x ", records[i].key, records[i].ptr);

        printf("\n%x ", hdr.sibling_ptr);

        printf("\n");
    }

    // print the subtree of this node
    void printAll()
    {
        if (hdr.leftmost_ptr == NULL)
        {
            printf("printing leaf node: ");
            print();
        }
        else
        {
            printf("printing internal node: ");
            print();
            ((page *)hdr.leftmost_ptr)->printAll();
            for (int i = 0; records[i].ptr != NULL; ++i)
            {
                ((page *)records[i].ptr)->printAll();
            }
        }
    }
}; // class page

bnode *alloc_bnode()
{
    create_bnode[thread_id]++;
#ifdef DRAM_SPACE_TEST
    __sync_fetch_and_add(&dram_space, sizeof(bnode));
#endif

    void *res = malloc(sizeof(bnode));
    memset(res, 0, sizeof(bnode));
    return (bnode *)res;
}

lnode *alloc_lnode()
{
    count_lnode_group[thread_id]++;

    return (lnode *)nvmpool_alloc(sizeof(lnode));
}

void dealloc_lnode(lnode *ln)
{
    count_lnode_group[thread_id]--;
    nvmpool_free(ln);
}

std::future<void> bg_thread;
volatile bool signal_run_bgthread;

__thread int count_add_log = 0;
inline void insert_into_logs(uint64_t key, uint64_t ptr, bool gc)
{
    if (!gc)
    {
#ifdef NUMA_TEST
        logpool_add_log(key, ptr);
#else
        add_log(key, ptr);
#endif
        count_add_log++;
        if ((count_add_log % 10000 == 0) && signal_do_recycle == false)
        {
            if (if_log_recycle())
            {

                if (__sync_bool_compare_and_swap(&signal_do_recycle, false, true))
                {
                    // printf("signal_do_recycle = %d\n", signal_do_recycle);
                }
                // signal_do_recycle = true;
            }
            count_add_log = 0;
        }
    }
    else
    {
#ifdef NUMA_TEST
        logpool_add_log(key, ptr);
#else
        add_log(key, ptr);
#endif
        ;
    }
}

/*
 * class btree
 */
btree::btree()
{

    first_inode = new page(); // level=0;
    root = (char *)first_inode;

    height = 0;

    bnode *first_bnode = alloc_bnode();
    first_lnode = alloc_lnode();

    first_bnode->meta.v.ptr = (uint64_t)first_lnode;
    first_inode->hdr.leftmost_ptr = (uint64_t)first_bnode;

    first_lnode->meta.bitmap = 1; // Insert (0,0) as a minimum kv to prevent the deletion of the first leaf node.

    epoch_num = 0;

    signal_do_recycle = false;

    signal_run_bgthread = true;
    _mm_mfence();
    bg_thread = std::async(
        std::launch::async, [&]()
        {
#ifdef PIN_CPU
                pin_cpu_core(num_threads);
#endif

            worker_id = num_threads;
            thread_id = num_threads;

            while (signal_run_bgthread)
            {
                // printf("%d\n",signal_do_recycle);
                if (signal_do_recycle == true)
                {
                    recycle_bottom();
                }
            } });
}

btree::~btree()
{
    ;
}

void btree::setNewRoot(char *new_root)
{
    this->root = (char *)new_root;
    ++height;
}

static inline unsigned char hashcode1B(key_type_sob x)
{
    x ^= x >> 32;
    x ^= x >> 16;
    x ^= x >> 8;
    return (unsigned char)(x & 0x0ffULL);
}

// return -1 if not find
inline int search_from_lnode(unsigned char key_hash, lnode *ln, key_type_sob key)
{

    // SIMD comparison
    // a. set every byte to key_hash in a 16B register
    __m128i key_16B = _mm_set1_epi8((char)key_hash);

    // b. load meta into another 16B register
    __m128i fgpt_16B = _mm_load_si128((const __m128i *)(ln->meta.fgpt));

    // c. compare them
    __m128i cmp_res = _mm_cmpeq_epi8(key_16B, fgpt_16B);

    // d. generate a mask
    unsigned int mask = (unsigned int)
        _mm_movemask_epi8(cmp_res); // 1: same; 0: diff

    // remove the lower 2 bits then AND bitmap
    // mask = (mask >> 2) & ((unsigned int)(ln->meta.bitmap));
    mask = mask & ((unsigned int)(ln->meta.bitmap)); // note : the higher 2 bits need to clear.

    // search every matching candidate
    while (mask)
    {
        int jj = bitScan(mask) - 1; // next candidate

        if (ln->k(jj) == key)
        { // found: do nothing, return
            return jj;
        }

        mask &= ~(0x1 << jj); // remove this bit
    }                         // end while

    return -1;
}

bnode *btree::get_the_target_bnode(entry_key_t key, uint8_t op_type, bnode **pred, page **inode)
{
    page *p = (page *)root;

    while (p->hdr.level != 0)
    {
        p = (page *)p->linear_search(key);
    }

    page *t;
    uint8_t previous_verion;
    while ((t = (page *)p->linear_search_last_level_pred(key, op_type, previous_verion, pred)) == (page *)(p->hdr.sibling_ptr))
    {
        p = t;
        if (!p)
        {
            break;
        }
    }

    *inode = p;
    if (!t)
    {
        assert(false);
        return NULL;
    }
    return (bnode *)t;
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level)
{
    if (level > ((page *)root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while (p->hdr.level > level)
        p = (page *)p->linear_search(key);

    if (!p->store(this, key, right, true))
    {
        btree_insert_internal(left, key, right, level);
    }
}

char *btree::search(entry_key_t key)
{

    page *inode = NULL;
    bnode *bn = get_the_target_bnode(key, 1, NULL, &inode);

    uint8_t previous_verion = bn->meta.v.version;
    // search bnode and lnode;

    // 2.5 search cache
    {
        // sequential search (which is slightly faster now)
        for (int i = 0; i < CACHE_KEY_NUM; i++)
            if (key == bn->cache[i].k)
            {
                return bn->cache[i].v;
            }
    }

    // 3. search leaf node
    lnode *ln = (lnode *)bn->meta.v.ptr;

    int ret_pos = search_from_lnode(hashcode1B(key), ln, key);

    if (previous_verion == bn->meta.v.version)
    {
        return ret_pos == -1 ? NULL : ln->ch(ret_pos);
    }
    else
    {
        return search(key);
    }
}

bool btree::insert(entry_key_t key, char *val, bool update)
{

    bnode *bnode_sibp = NULL;
    page *parent = NULL;
    bnode *bn = get_the_target_bnode(key, 0, &bnode_sibp, &parent);

    short cpos = -1;

    int i, b;
    uint8_t version;

    // search the buffer node

    cpos = bn->meta.v.counter;
    for (b = 0; b < bn->meta.v.counter; b++)
    {
        if (bn->cache[b].k == key)
        {
            if (update) // update operation, overwrite the target kv.
            {
                cpos = b;
                break;
            }
            else // find the key, just return
            {
                reset_lock_bnode(bn, 0);

                return true;
            }
        }
    }

    if (cpos < CACHE_KEY_NUM) // update the buffer node without accessing the leaf node.
    {
        bn->cache[cpos].k = key;
        bn->cache[cpos].v = val;
        if (cpos == bn->meta.v.counter) // insert this kv in a new empty slot
        {
            bn->meta.v.counter++;
        } // otherwise, this kv has been cached in the buffer node, just update the value
        if (epoch_num)
            bn->meta.v.epoch_num |= (1ULL << cpos);
        else
            bn->meta.v.epoch_num &= (~(1ULL << cpos));

        insert_into_logs(key, (uint64_t)val, false);

        reset_lock_bnode(bn, 0);

        return true;
    }
    else // the buffer node is full, insert this kv into the leaf node.
    {
        insert_into_leaf(bn, parent, key, val, update);

        reset_lock_bnode(bn, 0);

        return true;
    }
}

bool btree::insert_into_leaf(bnode *bn, page *parent, entry_key_t key, char *val, bool update)
{
    leaf_entry key_group[CACHE_KEY_NUM + 1];
    unsigned char key_hash_group[CACHE_KEY_NUM + 1];
    int8_t slot_id[CACHE_KEY_NUM + 1];
    int increment = 0;

    // 3. search leaf node
    lnode *ln = (lnode *)bn->meta.v.ptr;

#ifdef NUMA_TEST
    (the_logpool.vlog_groups->flushed_count[the_logpool.vlog_groups->alt]) += CACHE_KEY_NUM;
#else
    (vlog_groups[thread_id]->flushed_count[vlog_groups[thread_id]->alt]) += CACHE_KEY_NUM;
#endif
    // Calculate the number of flushed kvs, because deletion and insertion operations are mixed together.
    // It is necessary to determine whether this node is deleted or split.
    {

        unsigned char key_hash = hashcode1B(key);

        slot_id[0] = search_from_lnode(key_hash, ln, key);

        if ((update == false) && (slot_id[0] != -1)) // find the target kv in the leaf node, just return.
        {
            reset_lock_bnode(bn, 0);
            return true;
        }

        key_group[0].k = key;
        key_group[0].v = val;
        key_hash_group[0] = key_hash;

        for (int i = 0; i < CACHE_KEY_NUM; i++)
        {

            key_hash = hashcode1B(bn->cache[i].k);
            slot_id[i + 1] = search_from_lnode(key_hash, ln, bn->cache[i].k);
            key_group[i + 1] = bn->cache[i];
            key_hash_group[i + 1] = key_hash;
        }

        // Just reset the counter and reserve cached kvs in the buffer nodes until overwritten to accelerate search ops.
        bn->meta.v.counter = 0;
    }

#ifdef TREE_NO_SELECLOG
    insert_into_logs(key, (uint64_t)val, false);
#endif

    lnodeMeta meta;
    meta = ln->meta;

    bool need_to_flush[4];
    need_to_flush[0] = true;
    need_to_flush[1] = false;
    need_to_flush[2] = false;
    need_to_flush[3] = false;

    int index, slot;

    // First, handle the update and delete operations
    for (index = 0; index < CACHE_KEY_NUM + 1; index++)
    {
        if (slot_id[index] != -1)
        {
            if (key_group[index].v == 0) // delete
            {
                meta.bitmap &= (~(1 << slot_id[index]));
            }
            else // update
            {
                ln->ent[slot_id[index]].v = key_group[index].v;
                need_to_flush[(slot_id[index] + 2) / 4] = true;
            }
        }
    }

    // Second, handle the insertion operations.
    // Splitting operations may be triggered.
    for (index = 0; index < CACHE_KEY_NUM + 1; index++)
    {
        if (slot_id[index] == -1 && key_group[index].v != 0)
        {
            if (meta.bitmap != 0x3fff)
            {
                slot = bitScan(~meta.bitmap) - 1;
                ln->ent[slot] = key_group[index];
                meta.fgpt[slot] = key_hash_group[index];
                meta.bitmap |= (1 << slot);
                need_to_flush[(slot + 2) / 4] = true;
            }
            else
            {
                goto split_leaf_node;
            }
        }
    }

    {
    no_split_in_leaf_node:
        // flush the line containing slot and next pointer
        for (int cacheline_number = 3; cacheline_number >= 1; cacheline_number--)
        {
            if (need_to_flush[cacheline_number])
                clflush_nofence((char *)ln + cacheline_number * 64, CACHE_LINE_SIZE);
        }

        sfence();

        meta.timestamp = _rdtsc();
        ln->setMeta(&meta);
        clflush(ln, CACHE_LINE_SIZE);

        return true;
    }

    // The old leaf node will split.
    key_type_sob newkey;
    char *newptr;

    // The cached kvs in the buffer node also need to be divided into two parts.
    leaf_entry old_cache[CACHE_KEY_NUM];
    leaf_entry new_cache[CACHE_KEY_NUM];

    {
    split_leaf_node:

        // get sorted positions
        int timestamp = _rdtsc();
        int sorted_pos[LEAF_KEY_NUM];
        for (int i = 0; i < LEAF_KEY_NUM; i++)
            sorted_pos[i] = i;
        qsortBleaf(ln, 0, LEAF_KEY_NUM - 1, sorted_pos);

        // get cached kvs.
        for (int i = 0; i < CACHE_KEY_NUM; i++)
        {
            old_cache[i] = ln->ent[sorted_pos[i]];
            new_cache[i] = ln->ent[sorted_pos[i + (LEAF_KEY_NUM / 2)]];
        }

        // split point is the middle point
        int split = (LEAF_KEY_NUM / 2); // [0,..split-1] [split,LEAF_KEY_NUM-1]
        key_type_sob split_key = ln->k(sorted_pos[split]);

        // create new node
        lnode *newln = alloc_lnode();
        bnode *newbn = alloc_bnode();
        newbn->meta.v.ptr = (uint64_t)newln;

        // 2.4 move entries sorted_pos[split .. LEAF_KEY_NUM-1]
        uint16_t freed_slots = 0;
        for (int i = split; i < LEAF_KEY_NUM; i++)
        {
            newln->ent[i] = ln->ent[sorted_pos[i]];
            newln->meta.fgpt[i] = meta.fgpt[sorted_pos[i]];

            freed_slots |= (1 << sorted_pos[i]);
        }
        newln->meta.bitmap = (((1 << (LEAF_KEY_NUM - split)) - 1) << split);
        newln->meta.next = ln->meta.next;
        newln->meta.timestamp = timestamp;

        meta.bitmap &= ~freed_slots;

        int remaining_index[CACHE_KEY_NUM + 1];
        int remaining_num = 0;

        // insert the remaining kvs to the new leaf node
        for (; index < CACHE_KEY_NUM + 1; index++)
        {
            if (slot_id[index] == -1 && key_group[index].v != 0)
            {
                // key > split_key: insert kvs into the new node
                if (key_group[index].k >= split_key)
                {
                    slot = bitScan(~(newln->meta.bitmap)) - 1;
                    newln->ent[slot] = key_group[index];
                    newln->meta.fgpt[slot] = key_hash_group[index];
                    newln->meta.bitmap |= (1 << slot);
                }
                else
                {
                    remaining_index[remaining_num++] = index;
                }
            }
        }

        // persist the new leaf node
        clflush(newln, sizeof(lnode));

        // persist the data region of the old leaf node
        for (int cacheline_number = 3; cacheline_number >= 1; cacheline_number--)
        {
            if (need_to_flush[cacheline_number])
                clflush_nofence((char *)ln + cacheline_number * 64, CACHE_LINE_SIZE);
        }
        sfence();

        meta.next = (uint64_t)newln;
        meta.timestamp = timestamp;

        // update the meta region and persist it
        ln->setMeta(&meta);
        clflush(ln, CACHE_LINE_SIZE);

        // insert the remaining kvs to the old leaf nodes.
        {
            need_to_flush[0] = true;
            need_to_flush[1] = false;
            need_to_flush[2] = false;
            need_to_flush[3] = false;

            for (index = 0; index < remaining_num; index++)
            {
                slot = bitScan(~meta.bitmap) - 1;
                ln->ent[slot] = key_group[remaining_index[index]];
                meta.fgpt[slot] = key_hash_group[remaining_index[index]];
                meta.bitmap |= (1 << slot);
                need_to_flush[(slot + 2) / 4] = true;
            }

            for (int cacheline_number = 3; cacheline_number >= 1; cacheline_number--)
            {
                if (need_to_flush[cacheline_number])
                    clflush_nofence((char *)ln + cacheline_number * 64, CACHE_LINE_SIZE);
            }
            sfence();

            ln->setMeta(&meta);
            clflush(ln, CACHE_LINE_SIZE);
        }

        // new entry to be inserted into the inner node.
        newkey = split_key;
        newptr = (char *)newbn;

        {
            // update the cached kvs in buffer nodes
            for (int i = 0; i < CACHE_KEY_NUM; i++)
            {
                bn->cache[i] = old_cache[i];
                newbn->cache[i] = new_cache[i];
            }
            sfence();
        }
    }

    // nonleaf node
    {
        parent->store(this, newkey, newptr, true);

        return true;
    }
}

#pragma GCC push_options
#pragma GCC optimize("O0")
inline void wait_for_lock(bnode *bn, uint8_t op_type, uint8_t &version)
{
    while (!get_lock_bnode(bn, op_type, version)) // bn has been locked, waitting.
    {
        ;
    }
}
#pragma GCC pop_options

void btree::recycle_bottom()
{
// initialize new log files
#ifdef NUMA_TEST
    for (int i = 0; i < NUM_NUMA_NODE; i++)
    {
        for (int j = 0; j <= num_threads; j++)
        {
            per_numa_log_pool[i].thread_log_pool[j].switch_alt_and_init();
        }
    }
#else
    for (int i = 0; i <= num_threads; i++)
    {
        switch_alt_and_init(i);
    }
#endif

    sfence();
    epoch_num = 1 - epoch_num; // flip the global epoch
    sfence();

    page *current_inode = first_inode;
    bnode *bn;
    volatile bnode *bn_vo;
    volatile bool ret;
    uint8_t version;

    while (current_inode)
    {
        /* leaf most ptr*/
        if (current_inode->hdr.leftmost_ptr == NULL)
        {
            assert(false && "error,leafmost_ptr == NULL!");
        }

        /* get lock of bn*/
        bn = (bnode *)current_inode->hdr.leftmost_ptr;
        wait_for_lock(bn, 0, version);

        for (int i_cache = 0; i_cache < bn->meta.v.counter; i_cache++)
        {
            if (((bn->meta.v.epoch_num >> i_cache) & 1) != epoch_num)
            {
                insert_into_logs(bn->cache[i_cache].k, (uint64_t)bn->cache[i_cache].v, true);
                if (epoch_num)
                    bn->meta.v.epoch_num |= (1 << i_cache);
                else
                    bn->meta.v.epoch_num &= (~(1 << i_cache));
            }
        }

        reset_lock_bnode(bn, 0);

        /* bn in records[] */
        for (int i = 0; current_inode->records[i].ptr != NULL; i++)
        {
            /* get lock of bn*/
            bn = (bnode *)current_inode->records[i].ptr;
            wait_for_lock(bn, 0, version);

            for (int i_cache = 0; i_cache < bn->meta.v.counter; i_cache++)
            {
                if (((bn->meta.v.epoch_num >> i_cache) & 1) != epoch_num)
                {
                    insert_into_logs(bn->cache[i_cache].k, (uint64_t)bn->cache[i_cache].v, true);
                    if (epoch_num)
                        bn->meta.v.epoch_num |= (1 << i_cache);
                    else
                        bn->meta.v.epoch_num &= (~(1 << i_cache));
                }
            }

            reset_lock_bnode(bn, 0);
        }

        /* lock the next inode; */
        current_inode = (page *)current_inode->hdr.sibling_ptr;
    } // loop of inode

#ifdef NUMA_TEST
    for (int i = 0; i < NUM_NUMA_NODE; i++)
    {
        per_numa_log_pool[i].collect_old_log_to_freelist();
    }
#else

    for (int i = 0; i <= num_threads; i++)
    {
        collect_old_log_to_freelist(i);
    }

#endif // NUMA_TEST

    signal_do_recycle = false;
}

thread_local std::vector<entry_key_t> key_in_bnode(CACHE_KEY_NUM);
inline void get_range_key_from_lnode(bnode *bn, entry_key_t min_key, bool &if_find_a_small_key, int &off, std::vector<value_type_sob> &buf)
{
    if (bn == NULL)
        return;

    uint8_t version;
    int i;
    lnode *ln;

    int old_off = off;

retry:
    wait_for_lock(bn, 1, version);
    key_in_bnode.clear();
    if_find_a_small_key = false;
    off = old_off;

    // search cache
    for (i = 0; i < bn->meta.v.counter; i++)
    {
        if (bn->cache[i].k >= min_key)
        {
            buf[off++] = (uint64_t)(bn->cache[i].v);
            key_in_bnode.push_back(bn->cache[i].k);
        }
        else
            if_find_a_small_key = true;
    }
    // 3. search leaf node
    ln = (lnode *)bn->meta.v.ptr;
    for (i = 0; i < LEAF_KEY_NUM; i++)
    {
        if (ln->meta.bitmap & (1 << i))
        {
            if (ln->ent[i].k >= min_key)
            {
                if (std::find(key_in_bnode.begin(), key_in_bnode.end(), ln->ent[i].k) == key_in_bnode.end())
                    buf[off++] = (uint64_t)(ln->ent[i].v);
            }
            else
                if_find_a_small_key = true;
        }
    }

    if (version != bn->meta.v.version)
        goto retry;

    return;
}

// Range operation with linear search
int btree::btree_search_range(entry_key_t min_key, uint64_t len, std::vector<value_type_sob> &buf)
{
    page *p = (page *)root;

    while (p->hdr.level != 0)
    {
        p = (page *)p->linear_search(min_key);
    }

    int i, off = 0;
    uint8_t previous_switch_counter;
    page *current = p;
    bool if_find_a_small_key;

    while (current && off < len)
    {
        int old_off = off;
        do
        {
            previous_switch_counter = current->hdr.switch_counter;
            off = old_off;

            entry_key_t tmp_key;
            char *tmp_ptr;

            if (IS_FORWARD(previous_switch_counter))
            {
                if ((tmp_key = current->records[0].key) > min_key) // Starting from the far left
                {
                    get_range_key_from_lnode((bnode *)current->hdr.leftmost_ptr, min_key, if_find_a_small_key, off, buf);
                    if (off >= len)
                    {
                        goto end;
                    }
                }

                for (i = 0; current->records[i].ptr != NULL; ++i)
                {
                    if (current->records[i + 1].key < min_key)
                        continue;
                    else
                    {
                        get_range_key_from_lnode((bnode *)current->records[i].ptr, min_key, if_find_a_small_key, off, buf);
                        if (off >= len)
                        {
                            goto end;
                        }
                    }
                }
            }
            else
            {
                for (i = current->count() - 1; i > 0; --i)
                {
                    get_range_key_from_lnode((bnode *)current->records[i].ptr, min_key, if_find_a_small_key, off, buf);
                    if (if_find_a_small_key)
                    {
                        goto end;
                    }
                }

                get_range_key_from_lnode((bnode *)current->hdr.leftmost_ptr, min_key, if_find_a_small_key, off, buf);
                if (if_find_a_small_key)
                {
                    goto end;
                }
            }

        end:; // end to scan this inode.
        } while (previous_switch_counter != current->hdr.switch_counter);

        current = (page *)current->hdr.sibling_ptr;
    }

    return off;
}

void btree::printAll()
{
    int total_keys = 0;
    page *leftmost = (page *)root;
    int level = leftmost->hdr.level;

    printf("root: %x\n", root);
    while (level >= 0)
    {
        page *sibling = leftmost;
        while (sibling)
        {
            if (sibling->hdr.level == 0)
            {
                total_keys += sibling->hdr.last_index + 1;
            }
            sibling->print();
            sibling = (page *)sibling->hdr.sibling_ptr;
        }
        printf("-----------------------------------------\n");
        leftmost = (page *)leftmost->hdr.leftmost_ptr;
        level--;
    }

    printf("total number of keys: %d\n", total_keys);
}

void btree::printinfo_leaf()
{
    lnode *curr = first_lnode;

    while (curr)
    {
        uint16_t bitmap = curr->meta.bitmap;
        for (int i = 0; i < LEAF_KEY_NUM; i++)
        {
            if (bitmap & 1ULL << i)
            {
                printf("%llu ", curr->ent[i].k);
            }
        }
        printf("||");
        curr = (lnode *)curr->meta.next;
    }
}

void btree::clean_bottom()
{

// initialize new log files
#ifdef NUMA_TEST
    for (int i = 0; i < NUM_NUMA_NODE; i++)
    {
        for (int j = 0; j <= num_threads; j++)
        {
            per_numa_log_pool[i].thread_log_pool[j].switch_alt_and_init();
        }
    }
#else
    for (int i = 0; i <= num_threads; i++)
    {
        switch_alt_and_init(i);
    }
#endif

    page *current_inode = first_inode;
    bnode *bn;
    volatile bnode *bn_vo;
    volatile bool ret;
    uint8_t version;

    while (current_inode)
    {
        /* leaf most ptr*/
        if (current_inode->hdr.leftmost_ptr == NULL)
        {
            assert(false && "error,leafmost_ptr == NULL!");
        }

        bn = (bnode *)current_inode->hdr.leftmost_ptr;
        bn->meta.v.counter = 0;

        /* bn in records[] */
        for (int i = 0; current_inode->records[i].ptr != NULL; i++)
        {
            bn = (bnode *)current_inode->hdr.leftmost_ptr;
            bn->meta.v.counter = 0;
        }

        /* lock the next inode; */
        current_inode = (page *)current_inode->hdr.sibling_ptr;
    } // loop of inode

#ifdef NUMA_TEST
    for (int i = 0; i < NUM_NUMA_NODE; i++)
    {
        per_numa_log_pool[i].collect_old_log_to_freelist();
    }
#else

    for (int i = 0; i <= num_threads; i++)
    {
        collect_old_log_to_freelist(i);
    }

#endif // NUMA_TEST

    signal_do_recycle = false;
}
