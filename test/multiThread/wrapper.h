#pragma once

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define CHECK_RESULT

#if defined(FASTFAIR)
#include "btree.h"

btree *tree;
inline void tree_init()
{
    printf("init for multi-threads fast_fair!\n");
    tree = new btree();
};
inline void tree_end()
{
    delete tree;
};

inline void tree_insert(key_type_sob key)
{
#if defined(INSERT_REPEAT_KEY)
    tree->btree_insert(key, (char *)key, true);
#else
    tree->btree_insert(key, (char *)key, false);
#endif
};
inline void tree_search(key_type_sob key)
{
    char *res = tree->btree_search(key);
#ifdef CHECK_RESULT
    if (unlikely(res == NULL))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_update(key_type_sob key)
{
    tree->btree_insert(key, (char *)key, true);
};

inline void tree_delete(key_type_sob key)
{
    tree->btree_delete(key);
};

inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = tree->btree_search_range_2(min_key, length, buf); // todo

#ifdef CHECK_RESULT
    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
#endif
};

#elif defined(FPTREE)
#include "fptree.h"

fptree_t *tree;

inline void tree_init()
{
    printf("init for multi-threads fptree!\n");
    tree = fptree_create();
};

inline void tree_insert(key_type_sob key)
{
#if defined(INSERT_REPEAT_KEY)
    bool res = fptree_put(tree, key, (void *)key, true);
#else
    bool res = fptree_put(tree, key, (void *)key, false);
#endif

#ifdef CHECK_RESULT
    if (unlikely(res == false))
    {
        count_error_insert[thread_id]++;
    }
#endif
};

inline void tree_search(key_type_sob key) // bug
{
    bool res = fptree_get(tree, key);
#ifdef CHECK_RESULT
    if (unlikely(res == false))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_update(key_type_sob key)
{
    bool res = fptree_put(tree, key, (void *)key, true);
#ifdef CHECK_RESULT
    if (unlikely(res == false))
    {
        count_error_update[thread_id]++;

    }
#endif
};

inline void tree_delete(key_type_sob key)
{
    bool res = fptree_del(tree, key);
#ifdef CHECK_RESULT
    if (unlikely(res == false))
    {
        count_error_delete[thread_id]++;
    }
#endif
};

inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = fptree_scan(tree, min_key, length, buf);

    std::sort(buf.begin(), buf.end());


#ifdef CHECK_RESULT
    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
#endif
};

#elif defined(UTREE)
#include "utree.h"
btree *bt;

inline void tree_init()
{
    printf("init for multi-threads utree!\n");
    bt = new btree();
};

inline void tree_insert(key_type_sob key)
{
#if defined(INSERT_REPEAT_KEY)
    bt->insert(key, (char *)key, true);
#else
    bt->insert(key, (char *)key, false);
#endif
};
inline void tree_search(key_type_sob key)
{
    bool res = bt->search(key);
#ifdef CHECK_RESULT
    if (unlikely(res == false))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_update(key_type_sob key)
{
    bt->insert(key, (char *)key, true);
};
inline void tree_delete(key_type_sob key)
{
    bt->remove(key);
};
inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = bt->scan(min_key, length, buf);

#ifdef CHECK_RESULT
    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
#endif
};

#elif defined(LBTREE)
#include "lbtree.h"
lbtree *bt;

inline void tree_init()
{
    printf("init for multi-threads lbtree!\n");
    bt = new lbtree(false);
};

inline void tree_insert(key_type_sob key)
{
#if defined(INSERT_REPEAT_KEY)
    bt->insert(key, (char *)key, true);
#else
    bt->insert(key, (char *)key, false);
#endif
};

inline void tree_search(key_type_sob key)
{
    int index = 0;
    bleaf *lp = (bleaf *)bt->lookup(key, &index);
    value_type_sob res = lp->ch(index).value;
#ifdef CHECK_RESULT
    if (unlikely(res != key))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_update(key_type_sob key)
{
    bt->insert(key, (char *)key, true);
};
inline void tree_delete(key_type_sob key)
{
    bt->del(key);
};

inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = bt->scan(min_key, length, buf);

    std::sort(buf.begin(), buf.end());

    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
};

#elif defined(CCLBTREE_LB)
#include "cclbtree_lb.h"
tree *bt;

inline void tree_init()
{
    printf("init for multi-threads cclbtree_lb!\n");
    bt = new tree();
};

inline void tree_insert(key_type_sob key)
{
#if defined(INSERT_REPEAT_KEY)
    bt->insert_lnode(key, key, true);
#else
    bt->insert_lnode(key, key, false);
#endif
};

inline void tree_search(key_type_sob key)
{
    uint64_t res = bt->search_lnode(key);
#ifdef CHECK_RESULT

    if (unlikely(res != key))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_update(key_type_sob key)
{
    bt->insert_lnode(key, key, true);
};
inline void tree_delete(key_type_sob key)
{
    bt->insert_lnode(key, 0, true);
};

inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = bt->scan(min_key, length, buf);

    std::sort(buf.begin(), buf.end());

    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
};

inline void tree_recycle()
{
    bt->clean_bottom();
}

// #elif 1
#elif defined(DPTREE)
#include "concur_dptree.hpp"
dptree::concur_dptree<key_type_sob, value_type_sob> *bt;

inline void tree_init()
{
    printf("init for multi-threads dptree!\n");
    bt = new dptree::concur_dptree<key_type_sob, value_type_sob>();
};

inline void tree_end()
{
    delete bt;
}

inline void tree_insert(key_type_sob key)
{
    bt->insert(key, key);
};

inline void tree_search(key_type_sob key)
{
    bool res = bt->lookup(key, key);
#ifdef CHECK_RESULT

    // if (unlikely(res != key))
    // {
    //     printf("error: search.\n");
    //     exit(0);
    // }
    if (unlikely(res == false))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_update(key_type_sob key)
{
    bt->upsert(key, key); //
};
inline void tree_delete(key_type_sob key)
{
    bt->upsert(key, 1); //
};
inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = bt->lookup_range(min_key, length, buf);

    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
};

#elif defined(CCLBTREE_FF)
#include "cclbtree_ff.h"

btree *tree;
inline void tree_init()
{
    printf("init for multi-threads cclbtree_ff!\n");
    tree = new btree();
};
inline void tree_end()
{
    delete tree;
};

inline void tree_search(key_type_sob key)
{
    char *res = tree->search(key);
#ifdef CHECK_RESULT
    if (unlikely(res != (char *)key))
    {
        count_error_search[thread_id]++;
    }
#endif
};

inline void tree_insert(key_type_sob key)
{
#if defined(INSERT_REPEAT_KEY)
    tree->insert(key, (char *)key, true);
#else
    tree->insert(key, (char *)key, false);
#endif
};

inline void tree_update(key_type_sob key)
{
    tree->insert(key, (char *)key, true);
};

inline void tree_delete(key_type_sob key)
{
    tree->insert(key, (char *)0, true);
};

inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    int res = tree->btree_search_range(min_key, length, buf);

    std::sort(buf.begin(), buf.end());

#ifdef CHECK_RESULT
    if (unlikely(res == 0))
    {
        count_error_scan[thread_id]++;
    }
#endif
};

inline void tree_recycle()
{
    tree->clean_bottom();
}

#elif defined(PACTREE)

#include "pactree_wrapper.h"
pactree_wrapper *pt_wrapper;
thread_local key_type_sob global_key_ptr;
thread_local value_type_sob global_value_ptr;

inline void tree_init()
{
    printf("init for multi-threads pactree!\n");
    // pt_wrapper = new pactree_wrapper();
    tree_options_t tree_opt;
    tree_opt.pool_path = NVM_FILE_PATH1; // node 0
    pt_wrapper = reinterpret_cast<pactree_wrapper *>(create_tree(tree_opt));
};

inline void tree_end()
{
    delete pt_wrapper;
}

inline void tree_insert(key_type_sob key)
{
    global_key_ptr = key;
    global_value_ptr = key;
    pt_wrapper->insert(reinterpret_cast<char *>(&global_key_ptr), 8, reinterpret_cast<char *>(&global_value_ptr), 8);
};

inline void tree_search(key_type_sob key)
{
    global_key_ptr = key;
    pt_wrapper->find(reinterpret_cast<char *>(&global_key_ptr), 8, reinterpret_cast<char *>(&global_value_ptr));
};

inline void tree_update(key_type_sob key)
{
    global_key_ptr = key;
    global_value_ptr = key;
    pt_wrapper->update(reinterpret_cast<char *>(&global_key_ptr), 8, reinterpret_cast<char *>(&global_value_ptr), 8);
};

inline void tree_delete(key_type_sob key)
{
    global_key_ptr = key;
    pt_wrapper->remove(reinterpret_cast<char *>(&global_key_ptr), 8);
};

inline void tree_scan(key_type_sob min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
    global_key_ptr = min_key;
    // std::vector<char *> buf2;
    char *buf2;
    pt_wrapper->scan(reinterpret_cast<char *>(&global_key_ptr), 8, length, buf2);

    // sort in pactree_wrapper.cpp
};

inline void tree_get_memory_footprint() // only for pactree
{
    pt_wrapper->get_memory_footprint();
};

#endif
