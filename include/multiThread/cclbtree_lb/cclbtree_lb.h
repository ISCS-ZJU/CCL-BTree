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
#define OPEN_RTM

#define META(p) ((inodeMeta *)p)

/********************************************************/

volatile bool signal_do_recycle;

class lnode;
class Pointer8B;

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, lnode);
POBJ_LAYOUT_END(btree);

#define bitScan(x) __builtin_ffs(x)
#define countBit(x) __builtin_popcount(x)

void sfence()
{
    _mm_sfence();
    // _mm_mfence();
}

typedef struct leaf_entry
{
    key_type_sob k;
    value_type_sob v;
} leaf_entry;

typedef struct inode_entry
{
    key_type_sob k;
    uint64_t ptr;
} inode_entry;

/**
 * inode: non-leaf node
 *
 *   metadata (i.e. k(0))
 *
 *      k(1) .. k(NON_LEAF_KEY_NUM)
 *
 *   ch(0), ch(1) .. ch(NON_LEAF_KEY_NUM)
 */
typedef struct inodeMeta
{                       /* 8B */
    uint64_t next : 48; /* pointer to the next inode*/
    uint64_t lock : 1;  /* lock bit for concurrency control */
    uint64_t num : 15;  /* number of keys */
} inodeMeta;

class inode
{
public:
    inode_entry ent[NON_LEAF_KEY_NUM + 1];

public:
    key_type_sob &k(int idx) { return ent[idx].k; }
    uint64_t &ch(int idx) { return ent[idx].ptr; }
}; // inode

/**
 *  bottom node.   //a transfer station for leaf node
 *
 */
typedef struct bnode
{
    uint64_t ptr : 48; // pointer to leaf node
    uint64_t lock : 1;
    uint64_t epoch_num : 10; // for GC
    uint64_t counter : 5;
    leaf_entry cache[CACHE_KEY_NUM]; // the number of KVs that are not flushed to leaf nodes
} bnode;                             // bnode

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
    value_type_sob &ch(int idx) { return ent[idx].v; }

    int num() { return countBit(meta.bitmap); }

    bool isFull(void) { return (meta.bitmap == 0x3fff); }
    bool isAlmostFull(int a) { return (countBit(meta.bitmap) + a) > LEAF_KEY_NUM; }

    void setMeta(lnodeMeta *m)
    {
        memcpy(&meta, m, sizeof(lnodeMeta)); // should be 2 memcpy(addr , m , 8)
    }
}; // leafnode

class tree
{
public:
    int root_level;
    int epoch_num;
    inode *tree_root;

    inode *first_inode;
    lnode *first_lnode;

    void insert_lnode(key_type_sob key, value_type_sob val, bool update);
    value_type_sob search_lnode(key_type_sob key);
    int scan(key_type_sob minkey, uint64_t len, std::vector<value_type_sob> &buf);

    tree(bool is_recovery);
    ~tree();

    inode *alloc_inode();
    bnode *alloc_bnode();
    lnode *alloc_lnode();
    void dealloc_lnode(lnode *ln);

    void printinfo();
    void printinfo_leaf();
    void printinfo_bnode();

    void clean_bottom();

    void recycle_bottom();

    void recycle_bottom_naive();

    void recycle_bottom_naive_2();
};
/* ---------------------------------------------------------------------- */

inode *tree::alloc_inode()
{
#ifdef DRAM_SPACE_TEST
    __sync_fetch_and_add(&dram_space, sizeof(inode));
#endif

    return new inode();
}

bnode *tree::alloc_bnode()
{
    create_bnode[thread_id]++;
#ifdef DRAM_SPACE_TEST
    __sync_fetch_and_add(&dram_space, sizeof(bnode));
#endif

    void *res = malloc(sizeof(bnode));
    memset(res, 0, sizeof(bnode));
    return (bnode *)res;
}

lnode *tree::alloc_lnode()
{
    count_lnode_group[thread_id]++;
    return (lnode *)nvmpool_alloc(sizeof(lnode));
}

void tree::dealloc_lnode(lnode *ln)
{
    count_lnode_group[thread_id]--;

    nvmpool_free(ln);
}

std::future<void> bg_thread;
volatile bool signal_run_bgthread;
tree::tree(bool is_recovery = false)
{
    first_inode = alloc_inode();
    META(first_inode)->next = NULL;
    META(first_inode)->num = 0;
    META(first_inode)->lock = 0;

    bnode *first_bnode = alloc_bnode();
    first_lnode = alloc_lnode();

    tree_root = first_inode;
    first_bnode->ptr = (uint64_t)first_lnode;
    first_inode->ch(0) = (uint64_t)first_bnode;

    first_lnode->meta.bitmap = 1; // Insert (0,0) as a minimum kv to prevent the deletion of the first leaf node.

    root_level = 0;
    epoch_num = 0;

    signal_do_recycle = false;

    signal_run_bgthread = !is_recovery;
    _mm_mfence();
    bg_thread = std::async(
        std::launch::async, [&]()
        {
#ifdef PIN_CPU
            pin_cpu_core(num_threads);
#endif

            worker_id = num_threads;

            // printf("signal_run_bgthread = %d, begin!\n", signal_run_bgthread);
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

static unsigned char hashcode1B(key_type_sob x)
{
    x ^= x >> 32;
    x ^= x >> 16;
    x ^= x >> 8;
    return (unsigned char)(x & 0x0ffULL);
}

__thread int count_add_log = 0;
static void insert_into_logs(uint64_t key, uint64_t ptr, bool gc)
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

// return -1 if not find
static int search_from_lnode(unsigned char key_hash, lnode *ln, key_type_sob key)
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

/* ------------------------------------- *
   quick sort the keys in leaf node
 * ------------------------------------- */

// pos[] will contain sorted positions
static void qsortBleaf(lnode *p, int start, int end, int pos[])
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

/* ---------------------------------------------------------- */

value_type_sob tree::search_lnode(key_type_sob key)
{
    inode *in;
    bnode *bn;
    lnode *ln;

    int i, t, m, b;
    key_type_sob r;

    unsigned char key_hash = hashcode1B(key);
Again3:
#ifdef OPEN_RTM
    // 1. RTM begin
    if (_xbegin() != _XBEGIN_STARTED)
        goto Again3;
#endif

    sfence();
    // _mm_sfence();
    // 2. search nonleaf nodes
    in = tree_root;
    for (i = root_level; i >= 0; i--) // search from root to bottom.
    {
        // if the lock bit is set, abort
        if (META(in)->lock)
        {
#ifdef OPEN_RTM
            _xabort(ABORT_INODE);
#endif
            goto Again3;
        }

        // binary search to narrow down to at most 8 entries
        b = 1;
        t = META(in)->num;
        while (b + 7 <= t)
        {
            m = (b + t) >> 1;
            r = key - in->k(m);
            if (r > 0)
                b = m + 1;
            else if (r < 0)
                t = m - 1;
            else
            {
                in = (inode *)in->ch(m);
                goto inner_done;
            }
        }

        // sequential search (which is slightly faster now)
        for (; b <= t; b++)
            if (key < in->k(b))
                break;
        in = (inode *)in->ch(b - 1);
    inner_done:;
    }

    bn = (bnode *)in;

    // if the lock bit is set, abort
    if (bn->lock)
    {
#ifdef OPEN_RTM
        _xabort(ABORT_BNODE);
#endif
        goto Again3;
    }

    // 2.5 search cache
    {
        // sequential search (which is slightly faster now)
        for (b = 0; b < CACHE_KEY_NUM; b++)
            if (key == bn->cache[b].k)
            {
#ifdef OPEN_RTM
                _xend();
#endif
                return bn->cache[b].v;
            }
    }

    // 3. search leaf node
    ln = (lnode *)bn->ptr;

    int ret_pos = search_from_lnode(key_hash, ln, key);

#ifdef OPEN_RTM
    // 4. RTM commit
    _xend();
#endif

    return ret_pos == -1 ? 0 : ln->ch(ret_pos);
}

void tree::insert_lnode(key_type_sob key, value_type_sob val, bool update)
{

    // record the path from root to leaf
    // parray[level] is a node on the path
    // child ppos[level] of parray[level] == parray[level-1]
    //
    void *parray[32]; // 0 .. root_level will be used
    short ppos[32];   // 1 .. root_level will be used
    short cpos = -1;
    bool isfull[32]; // 0 .. root_level will be used

    inode *in = NULL;
    bnode *bn = NULL;
    lnode *ln = NULL;
    bnode *bnode_sibp = NULL;
    inode *inode_sibp = NULL;

    leaf_entry key_group[CACHE_KEY_NUM + 1];
    unsigned char key_hash_group[CACHE_KEY_NUM + 1];
    int8_t slot_id[CACHE_KEY_NUM + 1];
    int increment = 0;

    /* Part 1. get the positions to insert the key */

    {

        int i, t, m, b;
        key_type_sob r;

    Again3:
#ifdef OPEN_RTM
        // 1. RTM begin
        int stat = _xbegin();
        if (stat != _XBEGIN_STARTED)
        {
            // if (_XABORT_CODE(stat) == ABORT_BNODE)
            // count_conflict_in_bnode[thread_id]++;
            goto Again3;
        }

#endif
        sfence();
        // _mm_sfence();
        // 2. search nonleaf nodes
        in = tree_root;

        for (i = root_level; i >= 0; i--) // search from root to bottom.
        {

            // if the lock bit is set, abort
            if (META(in)->lock)
            {
#ifdef OPEN_RTM
                _xabort(ABORT_INODE);
#endif
                goto Again3;
            }

            parray[i] = in;
            isfull[i] = (META(in)->num == NON_LEAF_KEY_NUM);

            // binary search to narrow down to at most 8 entries
            b = 1;
            t = META(in)->num;
            while (b + 7 <= t)
            {
                m = (b + t) >> 1;
                r = key - in->k(m);
                if (r > 0)
                    b = m + 1;
                else if (r < 0)
                    t = m - 1;
                else
                {
                    in = (inode *)in->ch(m);
                    ppos[i] = m;
                    goto inner_done;
                }
            }

            // sequential search (which is slightly faster now)
            for (; b <= t; b++)
                if (key < in->k(b))
                    break;
            in = (inode *)in->ch(b - 1);
            ppos[i] = b - 1;
        inner_done:;
        }

        bn = (bnode *)in;

        // 2.5 search bottom leaf
        //  if the lock bit is set, abort
        if (bn->lock)
        {
#ifdef OPEN_RTM
            _xabort(ABORT_BNODE);
#endif
            goto Again3;
        }

        // 2.5 search the buffer node

        cpos = bn->counter;
        for (b = 0; b < bn->counter; b++)
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
#ifdef OPEN_RTM
                    _xend();
#endif
                    return;
                }
            }
        }

        if (cpos < CACHE_KEY_NUM) // update the buffer node without accessing the leaf node.
        {
            bn->lock = 1;
#ifdef OPEN_RTM
            _xend();
#endif

            bn->cache[cpos].k = key;
            bn->cache[cpos].v = val;
            if (cpos == bn->counter) // insert this kv in a new empty slot
            {
                bn->counter++;
            } // otherwise, this kv has been cached in the buffer node, just update the value
            if (epoch_num)
                bn->epoch_num |= (1ULL << cpos);
            else
                bn->epoch_num &= (~(1ULL << cpos));

            insert_into_logs(key, val, false);
            bn->lock = 0;

            return;
        }
        else // the buffer node is full, insert this kv into the leaf node.
        {
            // 3. search leaf node
            ln = (lnode *)bn->ptr;
#ifdef NUMA_TEST
            (the_logpool.vlog_groups->flushed_count[the_logpool.vlog_groups->alt]) += CACHE_KEY_NUM;
#else
            (vlog_groups[thread_id]->flushed_count[vlog_groups[thread_id]->alt]) += CACHE_KEY_NUM;
#endif
            // 3.5 Calculate the number of flushed kvs, because deletion and insertion operations are mixed together.
            // It is necessary to determine whether this node is deleted or split.
            {

                unsigned char key_hash = hashcode1B(key);

                slot_id[0] = search_from_lnode(key_hash, ln, key);

                if ((update == false) && (slot_id[0] != -1)) // find the target kv in the leaf node, just return.
                {
#ifdef OPEN_RTM
                    _xend();
                    return;
#endif
                }

                key_group[0].k = key;
                key_group[0].v = val;
                key_hash_group[0] = key_hash;

                for (i = 0; i < CACHE_KEY_NUM; i++)
                {

                    key_hash = hashcode1B(bn->cache[i].k);
                    slot_id[i + 1] = search_from_lnode(key_hash, ln, bn->cache[i].k);
                    key_group[i + 1] = bn->cache[i];
                    key_hash_group[i + 1] = key_hash;
                }

                // Just reset the counter and reserve cached kvs in the buffer nodes until overwritten to accelerate search ops.
                bn->counter = 0;

                // 3.5.2 count the kvs
                //   |           |   find    |  not find
                //   |   delete  |   -1      |   0
                //   |   insert  |   0       |   +1

                for (i = 0; i < CACHE_KEY_NUM + 1; i++)
                {
                    if (key_group[i].v == 0 && slot_id[i] != -1)
                        increment--;
                    else if (key_group[i].v != 0 && slot_id[i] == -1)
                        increment++;
                }
            }

            // 4. set lock bits before exiting the RTM transaction
            bn->lock = 1;

            if (ln->isAlmostFull(increment)) // split
            {

                for (i = 0; i <= root_level; i++)
                {
                    in = (inode *)parray[i];
                    META(in)->lock = 1;
                    if (!isfull[i])
                        break;
                }
            }
            else if (ln->num() + increment <= 0) // delete
            {
                // look for its bnode sibling and inode sibling

                inode *p = NULL;

                // from bottom to top
                for (i = 0; i <= root_level; i++)
                {
                    if (ppos[i] >= 1) // first leaf node will not be del, so i can not > root_level afte exitting the loop.
                        break;
                }

                p = (inode *)parray[i];
                inode_sibp = p;
                p = (inode *)p->ch(ppos[i] - 1);
                i--;

                for (; i >= 0; i--)
                {
                    if (META(p)->lock)
                    {
#ifdef OPEN_RTM
                        _xabort(ABORT_INODE);
#endif
                        goto Again3;
                    }

                    if (i == 0) // find the inode sibp
                        inode_sibp = p;
                    p = (inode *)p->ch(META(p)->num);
                }

                bnode_sibp = (bnode *)p;

                if (bnode_sibp->lock)
                {
#ifdef OPEN_RTM
                    _xabort(ABORT_BNODE);
#endif
                    goto Again3;
                }
                // lock bnode_sibp
                bnode_sibp->lock = 1;

                // lock the inode in level 0
                META(parray[0])->lock = 1;

                // inode will be deleted, lock the inode sibp
                if (META(parray[0])->num < 1)
                {
                    META(inode_sibp)->lock = 1;

                    // lock affected ancestors(inode)
                    for (i = 1; i <= root_level; i++)
                    {
                        p = (inode *)parray[i];
                        META(p)->lock = 1;

                        if (META(p)->num >= 1)
                            break; // at least 2 children, ok to stop
                    }
                }

#ifdef OPEN_RTM
                // 4. RTM commit
                _xend();
#endif
                goto delete_the_leaf_node;
            }
#ifdef OPEN_RTM
            // 4. RTM commit
            _xend();
#endif
        }
    }

#ifdef TREE_NO_SELECLOG
    insert_into_logs(key, val, false);
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
        // 1.4.2 flush the line containing slot and next pointer
        for (int cacheline_number = 3; cacheline_number >= 1; cacheline_number--)
        {
            if (need_to_flush[cacheline_number])
                clflush_nofence((char *)ln + cacheline_number * 64, CACHE_LINE_SIZE);
        }

        sfence();

        meta.timestamp = _rdtsc();

        ln->setMeta(&meta);
        clflush(ln, CACHE_LINE_SIZE);

        // unlock
        bn->lock = 0;

        return;
    }

    // The old leaf node will split.
    key_type_sob newkey;
    uint64_t newptr;

    // The cached kvs in the buffer node also need to be divided into two parts.
    leaf_entry old_cache[CACHE_KEY_NUM];
    leaf_entry new_cache[CACHE_KEY_NUM];

    {
    split_leaf_node:

        // 2.1 get sorted positions
        int timestamp = _rdtsc();
        int sorted_pos[LEAF_KEY_NUM];
        for (int i = 0; i < LEAF_KEY_NUM; i++)
            sorted_pos[i] = i;
        qsortBleaf(ln, 0, LEAF_KEY_NUM - 1, sorted_pos);

        // 2.1.1 get cached kvs.
        for (int i = 0; i < CACHE_KEY_NUM; i++)
        {
            old_cache[i] = ln->ent[sorted_pos[i]];
            new_cache[i] = ln->ent[sorted_pos[i + (LEAF_KEY_NUM / 2)]];
        }

        // 2.2 split point is the middle point
        int split = (LEAF_KEY_NUM / 2); // [0,..split-1] [split,LEAF_KEY_NUM-1]
        key_type_sob split_key = ln->k(sorted_pos[split]);

        // 2.3 create new node
        // lnode * newln = (lnode *)alloc_lnode(LEAF_SIZE);
        lnode *newln = alloc_lnode();
        bnode *newbn = alloc_bnode();
        newbn->ptr = (uint64_t)newln;

        // 2.4 move entries sorted_pos[split .. LEAF_KEY_NUM-1]
        uint16_t freed_slots = 0;
        for (int i = split; i < LEAF_KEY_NUM; i++)
        {
            newln->ent[i] = ln->ent[sorted_pos[i]];
            newln->meta.fgpt[i] = meta.fgpt[sorted_pos[i]]; // note:得用meta而不是ln->meta

            // add to freed slots bitmap
            freed_slots |= (1 << sorted_pos[i]);
        }
        newln->meta.bitmap = (((1 << (LEAF_KEY_NUM - split)) - 1) << split);
        newln->meta.next = ln->meta.next;
        newln->meta.timestamp = timestamp;

        // remove freed slots from temp bitmap
        meta.bitmap &= ~freed_slots;

        int remaining_index[CACHE_KEY_NUM + 1];
        int remaining_num = 0;

        // insert the remaining kvs to the new leaf node
        for (; index < CACHE_KEY_NUM + 1; index++)
        {
            if (slot_id[index] == -1 && key_group[index].v != 0)
            {
                // 2.5 key > split_key: insert key into new node
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
        newptr = (uint64_t)newbn;

        /* Part 2.5 bnode */
        {
            // update the cached kvs in buffer nodes
            for (int i = 0; i < CACHE_KEY_NUM; i++)
            {
                bn->cache[i] = old_cache[i];
                newbn->cache[i] = new_cache[i];
            }
            sfence();
            bn->lock = 0;
        }
    }

    /* Part 3. nonleaf node */
    {
        int n, i, pos, r, lev, total_level;

#define LEFT_KEY_NUM ((NON_LEAF_KEY_NUM) / 2)
#define RIGHT_KEY_NUM ((NON_LEAF_KEY_NUM)-LEFT_KEY_NUM)

        total_level = root_level;

        // update the inode level
        inode *p, *newin;
        lev = 0;
        while (lev <= total_level)
        {

            p = (inode *)parray[lev];
            n = META(p)->num;
            pos = ppos[lev] + 1; // the new child is ppos[lev]+1 >= 1

            /* if the non-leaf is not full, simply insert key ptr */

            if (n < NON_LEAF_KEY_NUM)
            {
                for (i = n; i >= pos; i--)
                    p->ent[i + 1] = p->ent[i];

                p->k(pos) = newkey;
                p->ch(pos) = newptr;
                META(p)->num = n + 1;
                sfence();

                // unlock after all changes are globally visible
                META(p)->lock = 0;

                return;
            }

            /* otherwise allocate a new non-leaf and redistribute the keys */
            newin = (inode *)alloc_inode();

            /* if key should be in the left node */
            if (pos <= LEFT_KEY_NUM)
            {
                for (r = RIGHT_KEY_NUM, i = NON_LEAF_KEY_NUM; r >= 0; r--, i--)
                {
                    newin->ent[r] = p->ent[i];
                }
                /* newin->key[0] actually is the key to be pushed up !!! */
                for (i = LEFT_KEY_NUM - 1; i >= pos; i--)
                    p->ent[i + 1] = p->ent[i];

                p->k(pos) = newkey;
                p->ch(pos) = newptr;
            }
            /* if key should be in the right node */
            else
            {
                for (r = RIGHT_KEY_NUM, i = NON_LEAF_KEY_NUM; i >= pos; i--, r--)
                {
                    newin->ent[r] = p->ent[i];
                }
                newin->k(r) = newkey;
                newin->ch(r) = newptr;
                r--;
                for (; r >= 0; r--, i--)
                {
                    newin->ent[r] = p->ent[i];
                }
            } /* end of else */

            newkey = newin->k(0);
            newptr = (uint64_t)newin;

            if (lev == 0) // update the inode list in the level 0.
            {
                META(newin)->next = META(p)->next;
                META(p)->next = (uint64_t)newin;
            }
            META(newin)->num = RIGHT_KEY_NUM;
            META(p)->num = LEFT_KEY_NUM;

            sfence();

            META(newin)->lock = 0;
            if (lev < total_level)
                META(p)->lock = 0; // do not clear lock bit of root

            lev++;
        } /* end of while loop */

        /* root was splitted !! add another level */
        newin = alloc_inode();

        META(newin)->num = 1;
        META(newin)->lock = 1;
        newin->ch(0) = (uint64_t)tree_root;
        newin->ch(1) = newptr;
        newin->k(1) = newkey;
        sfence(); // ensure new node is consistent

        inode *old_root = tree_root;
        root_level = lev;
        tree_root = newin;
        sfence(); // tree root change is globablly visible
                  // old root and new root are both locked

        // unlock old root
        META(old_root)->lock = 0;

        // unlock new root
        META(newin)->lock = 0;

        return;

#undef RIGHT_KEY_NUM
#undef LEFT_KEY_NUM
    }

    {
    delete_the_leaf_node:
#ifdef TREE_NO_SELECLOG
        insert_into_logs(key, val, false);
#endif

        // printf("del\n");
        assert(bnode_sibp);
        lnode *lnode_sibp = (lnode *)bnode_sibp->ptr;

        // remove it from sibling linked list
        lnode_sibp->meta.next = ln->meta.next;

        clflush(lnode_sibp, 8); // flush the pointer
        bnode_sibp->lock = 0;   // lock bit is not protected.

        dealloc_lnode(ln);
        free(bn);

        /* Part 3: non-leaf node */
        {

            inode *p;
            int n, i, pos, lev;
            lev = 0;

            while (1)
            {
                p = (inode *)parray[lev];
                n = META(p)->num;
                pos = ppos[lev];

                /* if the node has more than 1 children, simply delete */
                if (n > 0)
                {
                    if (pos == 0) // because k(0) is meta.
                    {
                        p->ch(0) = p->ch(1);
                        pos = 1; // move the rest
                    }
                    for (i = pos; i < n; i++)
                        p->ent[i] = p->ent[i + 1];
                    META(p)->num = n - 1;
                    sfence();
                    // all changes are globally visible now
                    META(p)->lock = 0;
                    return;
                }

                /* otherwise only 1 ptr */

                if (lev == 0)
                {
                    // update the inode list in level 0.
                    META(inode_sibp)->next = META(p)->next;
                    sfence();
                    META(inode_sibp)->lock = 0;
                }

                delete p;
                lev++;
            } /* end of while */
        }
    }
}

void tree::recycle_bottom()
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

    inode *in = first_inode;
    inode *next_in = NULL;
    bnode *bn;

    /* lock the first_inode; */
    {
    Again1:
#ifdef OPEN_RTM
        // 1. RTM begin
        if (_xbegin() != _XBEGIN_STARTED)
        {
            goto Again1;
        }
#endif
        sfence();
        if (META(in)->lock)
        {
#ifdef OPEN_RTM
            _xabort(ABORT_INODE);
#endif
            goto Again1;
        }

        META(in)->lock = 1;
#ifdef OPEN_RTM
        _xend();
#endif
    }
    while (true)
    {
        /* in has been locked*/
        for (int i_bnode = 0; i_bnode <= META(in)->num; i_bnode++)
        {
            bn = (bnode *)in->ch(i_bnode);
            volatile bnode *bn_vo = bn;
            while ((bn_vo->lock)) // bn has been locked, waitting.
            {
                ;
            }
            sfence();
            assert(META(in)->lock);
            assert(bn_vo->lock != 1);

            for (int i_cache = 0; i_cache < bn->counter; i_cache++)
            {
                if (((bn->epoch_num >> i_cache) & 1) != epoch_num)
                {
                    insert_into_logs(bn->cache[i_cache].k, bn->cache[i_cache].v, true);
                    if (epoch_num)
                        bn->epoch_num |= (1 << i_cache);
                    else
                        bn->epoch_num &= (~(1 << i_cache));
                }
            }
            assert(bn_vo->lock != 1);
        }
        /* lock the next inode; */
        {
        Again2:
#ifdef OPEN_RTM
            // 1. RTM begin
            if (_xbegin() != _XBEGIN_STARTED)
            {
                goto Again2;
            }
#endif
            sfence();
            next_in = (inode *)META(in)->next;
            if (!next_in)
            {
#ifdef OPEN_RTM
                _xend();
#endif
                META(in)->lock = 0;
                goto done;
            }
            if (META(next_in)->lock)
            {
#ifdef OPEN_RTM
                _xabort(ABORT_INODE);
#endif
                goto Again2;
            }

            META(next_in)->lock = 1;
#ifdef OPEN_RTM
            _xend();
#endif
        }
        META(in)->lock = 0;
        in = next_in;
    } // loop of bnode

done:

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

void tree::clean_bottom()
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

    inode *inode_curr = first_inode;
    bnode *bnode_curr;
    while (inode_curr)
    {
        for (int i = 0; i <= META(inode_curr)->num; i++)
        {
            bnode_curr = (bnode *)inode_curr->ch(i);
            bnode_curr->counter = 0;
        }

        inode_curr = (inode *)META(inode_curr)->next;
    }

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

int tree::scan(key_type_sob minkey, uint64_t len, std::vector<value_type_sob> &buf)
{
    bool is_first_node;
    inode *in;
    inode *cur_in;
    bnode *bn;
    lnode *ln;
    int i, t, m, b, i_bnode;
    key_type_sob r;

Again3:
    is_first_node = true;
#ifdef OPEN_RTM
    if (_xbegin() != _XBEGIN_STARTED)
        goto Again3;
#endif
    sfence();

    // 2. search nonleaf nodes
    in = tree_root;

    for (i = root_level; i >= 0; i--) // search from root to bottom.
    {

        // if the lock bit is set, abort
        if (META(in)->lock)
        {

#ifdef OPEN_RTM
            _xabort(ABORT_INODE);
#endif
            goto Again3;
        }

        // binary search to narrow down to at most 8 entries
        b = 1;
        t = META(in)->num;
        while (b + 7 <= t)
        {
            m = (b + t) >> 1;
            r = minkey - in->k(m);
            if (r > 0)
                b = m + 1;
            else if (r < 0)
                t = m - 1;
            else
            {
                cur_in = in;
                i_bnode = m;
                in = (inode *)in->ch(m);
                goto inner_done;
            }
        }

        // sequential search (which is slightly faster now)
        for (; b <= t; b++)
            if (minkey < in->k(b))
                break;
        cur_in = in;
        i_bnode = b - 1;
        in = (inode *)in->ch(b - 1);
    inner_done:;
    }

    bn = (bnode *)in;

    // if the lock bit is set, abort
    if (bn->lock)
    {
#ifdef OPEN_RTM
        _xabort(ABORT_BNODE);
#endif
        goto Again3;
    }

    while (buf.size() < len)
    {
        // search cache
        for (b = 0; b < bn->counter; b++)
        {
            if (is_first_node)
            {
                if (bn->cache[b].k >= minkey)
                    buf.push_back(bn->cache[b].v);
            }
            else
            {
                buf.push_back(bn->cache[b].v);
            }
        }
        // 3. search leaf node
        ln = (lnode *)bn->ptr;
        for (b = 0; b < LEAF_KEY_NUM; b++)
        {
            if (ln->meta.bitmap & (1 << b))
            {
                if (is_first_node)
                {
                    if (ln->ent[b].k >= minkey)
                        buf.push_back(ln->ent[b].v);
                }
                else
                {
                    buf.push_back(ln->ent[b].v);
                }
            }
        }

        i_bnode++;

        if (i_bnode > META(cur_in)->num)
        {
            cur_in = (inode *)META(cur_in)->next; // to the next bnode.
            if (!cur_in)
            { //  reaches the tail
#ifdef OPEN_RTM
                _xend();
#endif
                return buf.size();
            }
            if (META(cur_in)->lock)
            {
#ifdef OPEN_RTM
                _xabort(ABORT_INODE);
#endif
                goto Again3;
            }
            i_bnode = 0;
        }

        bn = (bnode *)cur_in->ch(i_bnode);

        is_first_node = false;
    }

#ifdef OPEN_RTM
    // 4. RTM commit
    _xend();
#endif
    return buf.size();
}

void tree::printinfo_leaf()
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

void tree::printinfo_bnode()
{
    inode *curr = first_inode;
    bnode *bn = NULL;
    bool first_printf = true;
    while (curr)
    {
        uint8_t num = META(curr)->num;
        for (int i = 0; i <= num; i++)
        {
            bn = (bnode *)curr->ch(i);
            if (first_printf)
            {
                printf("0");
                first_printf = false;
            }

            else
                printf("%llu", curr->k(i));
            for (int j = 0; j < CACHE_KEY_NUM; j++)
                printf("(%llu %llu)", bn->cache[j].k, bn->cache[j].v);
            printf(" ");
            // printf("( k = %llu, ptr = %p) ", curr->k(i), curr->ch(i));
        }
        printf("||");
        curr = (inode *)META(curr)->next;
    }
}