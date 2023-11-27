#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>
using namespace std;

#include "util.h"

/******************************************* The PCM emulate configuration ******************************************/
#define EXTRA_SCM_LATENCY 500
#define M_PCM_CPUFREQ 3900
#define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 1000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)
#define FLUSH_ALIGN ((uintptr_t)64)

#define USE_PM
#define USE_PMDK
#ifndef USE_PM
#define USE_DRAM
#endif

static inline unsigned long long asm_rdtsc(void)
{
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc"
                       : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}
static inline void emulate_latency_ns(uint64_t ns)
{
  /*
    uint64_t cycles, start, stop;
    start = asm_rdtsc();
    cycles = NS2CYCLE(ns);

    do {
      stop = asm_rdtsc();
    } while (stop - start < cycles);
  */
  return;
}

#define CPU_FREQ_MHZ (3900)
#define CACHE_LINE_SIZE 64
#define IS_FORWARD(c) (c % 2 == 0)
#ifndef UNIFIED_NODE
#define PAGESIZE 1024 // ori 1024
#else
#define PAGESIZE 256 // node size
#endif

#define entry_key_t int64_t
unsigned long long search_time_in_insert = 0;
unsigned int gettime_cnt = 0;
unsigned long long clflush_time_in_insert = 0;
unsigned long long update_time_in_insert = 0;
int clflush_cnt = 0;
int node_cnt = 0;
pthread_mutex_t print_mtx;

#if 0
const uint64_t SPACE_PER_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread char *start_addr;
extern __thread char *curr_addr;

static inline void cpu_pause()
{
    __asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;

    return var;
}

inline void mfence()
{
  asm volatile("mfence":::"memory");
}

// HQD ADD:
static inline void asm_clflush(volatile uint64_t *addr)
{
  asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)));
}

void clflush(char *addr, int len)
{
#ifdef USE_PM
  mfence();
  for (uintptr_t uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
       uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN) {
    asm_clflush((uint64_t *)uptr);
    emulate_latency_ns(EXTRA_SCM_LATENCY);
  }
  mfence();
#endif
}
#endif

class page;
#ifdef USE_PMDK
POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, page);
POBJ_LAYOUT_END(btree);
#endif

class btree
{
private:
  int height;
  char *root;

public:
  btree();
  ~btree();
  void setNewRoot(char *);
  void getNumberOfNodes();
  void btree_insert(entry_key_t, char *, bool update);
  void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
  void btree_delete(entry_key_t);
  void btree_delete_internal(entry_key_t, char *, uint32_t, entry_key_t *, bool *, page **);
  char *btree_search(entry_key_t);
  void btree_search_range(entry_key_t, entry_key_t, unsigned long *);
  int btree_search_range_2(entry_key_t minkey, uint64_t len, std::vector<value_type_sob> &buf);
  void printAll();

  friend class page;
};

class header
{
private:
  page *leftmost_ptr;     // 8 bytes
  page *sibling_ptr;      // 8 bytes
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes
  std::mutex *mtx;        // 8 bytes

  friend class page;
  friend class btree;

public:
  header()
  {
    mtx = new std::mutex();

    // if (fastfair_cnt == 0){
    //   std::cout << "sizeof(std::mutex)=" << sizeof(std::mutex) <<endl;
    // }
    // __sync_fetch_and_add(&fastfair_cnt, 1);

    leftmost_ptr = NULL;
    sibling_ptr = NULL;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }

  ~header()
  {
    delete mtx;
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
  header hdr;                 // header in persistent memory, 32 bytes
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
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;

    clflush((char *)this, sizeof(page));
  }

  void *operator new(size_t size)
  {
#ifdef USE_PM
    return nvmpool_alloc(size);
#endif
#ifdef USE_DRAM
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
#endif
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

        // flush
        uint64_t records_ptr = (uint64_t)(&records[i]);
        int remainder = records_ptr % CACHE_LINE_SIZE;
        bool do_flush = (remainder == 0) ||
                        ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) &&
                         ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
        if (do_flush)
        {

          clflush((char *)records_ptr, CACHE_LINE_SIZE);
        }
      }
    }

    if (shift)
    {
      --hdr.last_index;
    }
    return shift;
  }

  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true)
  {
    hdr.mtx->lock();

    bool ret = remove_key(key);

    hdr.mtx->unlock();

    return ret;
  }

  inline void
  insert_key(entry_key_t key, char *ptr, int *num_entries, bool flush = true,
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

      if (flush)
      {
        clflush((char *)this, CACHE_LINE_SIZE);
      }
    }
    else
    {
      int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;
      if (flush)
      {
        if ((uint64_t) & (records[*num_entries + 1].ptr) % CACHE_LINE_SIZE == 0)
          clflush((char *)&(records[*num_entries + 1].ptr), sizeof(char *));
      }

      // FAST
      for (i = *num_entries - 1; i >= 0; i--)
      {
        if (key < records[i].key)
        {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;

          if (flush)
          {
            uint64_t records_ptr = (uint64_t)(&records[i + 1]);

            int remainder = records_ptr % CACHE_LINE_SIZE;
            bool do_flush = (remainder == 0) ||
                            ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) && ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
            if (do_flush)
            {
              clflush((char *)records_ptr, CACHE_LINE_SIZE);
              to_flush_cnt = 0;
            }
            else
              ++to_flush_cnt;
          }
        }
        else
        {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;

          if (flush)
            clflush((char *)&records[i + 1], sizeof(entry));
          inserted = 1;
          break;
        }
      }
      if (inserted == 0)
      {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;
        if (flush)
          clflush((char *)&records[0], sizeof(entry));
      }
    }

    if (update_last_index)
    {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // Insert a new key - FAST and FAIR
  page *store(btree *bt, char *left, entry_key_t key, char *right,
              bool flush, bool with_lock, page *invalid_sibling = NULL, bool update = false)
  {
    if (with_lock)
    {
      hdr.mtx->lock(); // Lock the write lock
    }
    if (hdr.is_deleted)
    {
      if (with_lock)
      {
        hdr.mtx->unlock();
      }

      return NULL;
    }

    // If this node has a sibling node,
    if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling))
    {
      // Compare this key with the first key of the sibling
      if (key > hdr.sibling_ptr->records[0].key)
      {
        if (with_lock)
        {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        return hdr.sibling_ptr->store(bt, NULL, key, right,
                                      true, with_lock, invalid_sibling, update);
      }
    }

    register int num_entries = count();

    for (int i = 0; i < num_entries; i++)
      if (key == records[i].key)
      {
        // if found, update
        if (update)
        {
          records[i].ptr = right;
          clflush(&records[i].ptr, 8);
        }
        // Already exists, we don't need to do anything, just return.
        if (with_lock)
          hdr.mtx->unlock();
        return this;
      }
    // simulate the 64B-value-persist latency.
    //  emulate_latency_ns(EXTRA_SCM_LATENCY);

    // FAST
    if (num_entries < cardinality - 1)
    {
      insert_key(key, right, &num_entries, flush);

      if (with_lock)
      {
        hdr.mtx->unlock(); // Unlock the write lock
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
        for (int i = m; i < num_entries; ++i)
        {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
        }
      }
      else
      { // internal node
        for (int i = m + 1; i < num_entries; ++i)
        {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
        }
        sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
      }

      sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      clflush((char *)sibling, sizeof(page));

      hdr.sibling_ptr = sibling;
      clflush((char *)&hdr, sizeof(hdr));

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;
      clflush((char *)&records[m], sizeof(entry));

      hdr.last_index = m - 1;
      clflush((char *)&(hdr.last_index), sizeof(int16_t));

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
          hdr.mtx->unlock(); // Unlock the write lock
        }
      }
      else
      {
        if (with_lock)
        {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                  hdr.level + 1);
      }
      return ret;
    }
  }

  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max, unsigned long *buf)
  {
    int i, off = 0;
    uint8_t previous_switch_counter;
    page *current = this;

    while (current)
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
          if ((tmp_key = current->records[0].key) > min)
          {
            if (tmp_key < max)
            {
              if ((tmp_ptr = current->records[0].ptr) != NULL)
              {
                if (tmp_key == current->records[0].key)
                {
                  if (tmp_ptr)
                  {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            }
            else
              return;
          }

          for (i = 1; current->records[i].ptr != NULL; ++i)
          {
            if ((tmp_key = current->records[i].key) > min)
            {
              if (tmp_key < max)
              {
                if ((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr)
                {
                  if (tmp_key == current->records[i].key)
                  {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
              else
                return;
            }
          }
        }
        else
        {
          for (i = count() - 1; i > 0; --i)
          {
            if ((tmp_key = current->records[i].key) > min)
            {
              if (tmp_key < max)
              {
                if ((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr)
                {
                  if (tmp_key == current->records[i].key)
                  {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
              else
                return;
            }
          }

          if ((tmp_key = current->records[0].key) > min)
          {
            if (tmp_key < max)
            {
              if ((tmp_ptr = current->records[0].ptr) != NULL)
              {
                if (tmp_key == current->records[0].key)
                {
                  if (tmp_ptr)
                  {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            }
            else
              return;
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      current = current->hdr.sibling_ptr;
    }
  }

  // Search keys with linear search
  int linear_search_range_2(entry_key_t min, uint64_t length, std::vector<value_type_sob> &buf2)
  {
    int i, off = 0;
    uint8_t previous_switch_counter;
    page *current = this;
    uint64_t max = UINT64_MAX;

    value_type_sob buf[mmax_length_for_scan];

    while (current && (off < length))
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
          if ((tmp_key = current->records[0].key) > min)
          {
            if (tmp_key < max)
            {
              if ((tmp_ptr = current->records[0].ptr) != NULL)
              {
                if (tmp_key == current->records[0].key)
                {
                  if (tmp_ptr)
                  {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            }
            else
              goto end;
          }

          for (i = 1; current->records[i].ptr != NULL; ++i)
          {
            if ((tmp_key = current->records[i].key) > min)
            {
              if (tmp_key < max)
              {
                if ((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr)
                {
                  if (tmp_key == current->records[i].key)
                  {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
              else
                goto end;
            }
          }
        }
        else
        {
          for (i = count() - 1; i > 0; --i)
          {
            if ((tmp_key = current->records[i].key) > min)
            {
              if (tmp_key < max)
              {
                if ((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr)
                {
                  if (tmp_key == current->records[i].key)
                  {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
              else
                goto end;
            }
          }

          if ((tmp_key = current->records[0].key) > min)
          {
            if (tmp_key < max)
            {
              if ((tmp_ptr = current->records[0].ptr) != NULL)
              {
                if (tmp_key == current->records[0].key)
                {
                  if (tmp_ptr)
                  {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            }
            else
              goto end;
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      current = current->hdr.sibling_ptr;
    }

  end:
    for (int i = 0; i < off; i++)
    {
      buf2.push_back(buf[i]);
    }
    // printf("%d\n",off);
    return off;
  }

  char *linear_search(entry_key_t key)
  {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    if (hdr.leftmost_ptr == NULL)
    { // Search a leaf node
      do
      {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter))
        {
          if ((k = records[0].key) == key)
          {
            if ((t = records[0].ptr) != NULL)
            {
              if (k == records[0].key)
              {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i)
          {
            if ((k = records[i].key) == key)
            {
              if (records[i - 1].ptr != (t = records[i].ptr))
              {
                if (k == records[i].key)
                {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
        else
        { // search from right to left
          for (i = count() - 1; i > 0; --i)
          {
            if ((k = records[i].key) == key)
            {
              if (records[i - 1].ptr != (t = records[i].ptr) && t)
              {
                if (k == records[i].key)
                {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret)
          {
            if ((k = records[0].key) == key)
            {
              if (NULL != (t = records[0].ptr) && t)
              {
                if (k == records[0].key)
                {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret)
      {
        return ret;
      }

      if ((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
        return t;

      return NULL;
    }
    else
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

  // print a node
  void print()
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

    printf("%x ", hdr.sibling_ptr);

    printf("\n");
  }

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
};

/*
 * class btree
 */
btree::btree()
{
  root = (char *)new page();
  height = 1;
}

btree::~btree()
{
  ;
}

void btree::setNewRoot(char *new_root)
{
  this->root = (char *)new_root;
  clflush((char *)&(this->root), sizeof(char *));
  ++height;
}

char *btree::btree_search(entry_key_t key)
{
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL)
  {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr)
  {
    p = t;
    if (!p)
    {
      break;
    }
  }

  if (!t || (char *)t != (char *)key)
  {
    return NULL;
  }

  return (char *)t;
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char *right, bool update)
{ // need to be string
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL)
  {
    p = (page *)p->linear_search(key);
  }

  if (!p->store(this, NULL, key, right, true, true, NULL, update))
  { // store
    btree_insert(key, right, update);
  }
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level)
{
  if (level > ((page *)root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level)
    p = (page *)p->linear_search(key);

  if (!p->store(this, NULL, key, right, true, true))
  {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key)
{
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL)
  {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr)
  {
    p = t;
    if (!p)
      break;
  }

  if (p)
  {
    if (!p->remove(this, key))
    {
      return; // todo: source code has bug, we just return.
      // btree_delete(key);
    }
  }
  else
  {
    printf("not found the key to delete %lu\n", key);
  }
}

void btree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key,
                                  bool *is_leftmost_node, page **left_sibling)
{
  if (level > ((page *)this->root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level)
  {
    p = (page *)p->linear_search(key);
  }

  p->hdr.mtx->lock();

  if ((char *)p->hdr.leftmost_ptr == ptr)
  {
    *is_leftmost_node = true;
    p->hdr.mtx->unlock();
    return;
  }

  *is_leftmost_node = false;

  for (int i = 0; p->records[i].ptr != NULL; ++i)
  {
    if (p->records[i].ptr == ptr)
    {
      if (i == 0)
      {
        if ((char *)p->hdr.leftmost_ptr != p->records[i].ptr)
        {
          *deleted_key = p->records[i].key;
          *left_sibling = p->hdr.leftmost_ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
      else
      {
        if (p->records[i - 1].ptr != p->records[i].ptr)
        {
          *deleted_key = p->records[i].key;
          *left_sibling = (page *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min, entry_key_t max, unsigned long *buf)
{
  page *p = (page *)root;

  while (p)
  {
    if (p->hdr.leftmost_ptr != NULL)
    {
      // The current page is internal
      p = (page *)p->linear_search(min);
    }
    else
    {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

// Function to search keys from "min" to "max"
int btree::btree_search_range_2(entry_key_t min_key, uint64_t length, std::vector<value_type_sob> &buf)
{
  page *p = (page *)root;

  while (p)
  {
    if (p->hdr.leftmost_ptr != NULL)
    {
      // The current page is internal
      p = (page *)p->linear_search(min_key);
    }
    else
    {
      // Found a leaf
      return p->linear_search_range_2(min_key, length, buf);
    }
  }
}

void btree::printAll()
{
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  page *leftmost = (page *)root;
  printf("root: %x\n", root);
  do
  {
    page *sibling = leftmost;
    while (sibling)
    {
      if (sibling->hdr.level == 0)
      {
        total_keys += sibling->hdr.last_index + 1;
      }
      sibling->print();
      sibling = sibling->hdr.sibling_ptr;
    }
    printf("-----------------------------------------\n");
    leftmost = leftmost->hdr.leftmost_ptr;
  } while (leftmost);

  printf("total number of keys: %d\n", total_keys);
  pthread_mutex_unlock(&print_mtx);
}
