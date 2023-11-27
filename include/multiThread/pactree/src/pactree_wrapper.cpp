#include "pactree_wrapper.h"
#include <atomic>

std::atomic<uint64_t> dram_allocated(0);
std::atomic<uint64_t> pmem_allocated(0);
std::atomic<uint64_t> dram_freed(0);
std::atomic<uint64_t> pmem_freed(0);

size_t pool_size_ = ((size_t)(1024 * 1024 * 40) * 1024);
std::string *pool_dir_;

extern "C" tree_api* create_tree(const tree_options_t& opt) {
	auto path_ptr = new std::string(opt.pool_path);
    if (*path_ptr != "")
    	pool_dir_ = path_ptr;
    else
		pool_dir_ = new std::string("./");
	
    if (opt.pool_size != 0)
    	pool_size_ = opt.pool_size;

    printf("PMEM Pool Dir: %s\n", pool_dir_->c_str());
    printf("PMEM Pool size: %lld\n", pool_size_);
	return new pactree_wrapper();
}


thread_local bool pactree_wrapper::thread_init = false;

void pactree_thread_init(pactree * tree) // add by hpy
{
    if (!pactree_wrapper::thread_init) {
        tree->registerThread();
        pactree_wrapper::thread_init = true;
    }
}

// struct ThreadHelper
// {
//     ThreadHelper(pactree* t){
//         t->registerThread();
// 	// int id = omp_get_thread_num();
//         // printf("Thread ID: %d\n", id);
//     }
//     ~ThreadHelper(){}
    
// };

pactree_wrapper::pactree_wrapper()
{
    tree_ = new pactree(1);
}

pactree_wrapper::~pactree_wrapper()
{
#ifdef MEMORY_FOOTPRINT
    // printf("DRAM Allocated: %llu\n", dram_allocated.load());
    // printf("DRAM Freed: %llu\n", dram_freed.load());
    printf("PMEM Allocated: %llu\n", pmem_allocated.load());
    // printf("PMEM Freed: %llu\n", pmem_freed.load());
#endif
    if (tree_ != nullptr)
        delete tree_;
    //tree_ = nullptr;
}


bool pactree_wrapper::find(const char* key, size_t key_sz, char* value_out)
{
    //thread_local ThreadHelper t(tree_); //add by zhenxin
    pactree_thread_init(tree_); // add by hpy
    Val_t value = tree_->lookup(*reinterpret_cast<Key_t*>(const_cast<char*>(key)));
    if (value == 0)
    {
        return false;
    }
    memcpy(value_out, &value, sizeof(value));
    return true;
}

bool pactree_wrapper::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    //thread_local ThreadHelper t(tree_); //add by zhenxin
    pactree_thread_init(tree_); // add by hpy
    if (!tree_->insert(*reinterpret_cast<Key_t*>(const_cast<char *>(key)), *reinterpret_cast<Val_t*>(const_cast<char *>(value))))
    {
        return false;
    }
    return true;
}

bool pactree_wrapper::update(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    //thread_local ThreadHelper t(tree_); //add by zhenxin
    pactree_thread_init(tree_); // add by hpy
    if (!tree_->update(*reinterpret_cast<Key_t*>(const_cast<char*>(key)), *reinterpret_cast<Val_t*>(const_cast<char*>(value))))
    {
        return false;
    }
    return true;
}

bool pactree_wrapper::remove(const char* key, size_t key_sz) {
    //thread_local ThreadHelper t(tree_); //add by zhenxin
    pactree_thread_init(tree_); // add by hpy
    if (!tree_->remove(*reinterpret_cast<Key_t*>(const_cast<char*>(key))))
    {
        return false;
    }
    return true;
}

int pactree_wrapper::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out)
{
    //thread_local ThreadHelper t(tree_); //add by zhenxin
    pactree_thread_init(tree_); // add by hpy
    constexpr size_t ONE_MB = 1ULL << 20;
    //static thread_local char results[ONE_MB];
    thread_local std::vector<Val_t> results;
    results.reserve(scan_sz);
    int scanned = tree_->scan(*reinterpret_cast<Key_t*>(const_cast<char*>(key)), (uint64_t)scan_sz, results);
//    if (scanned < 100)
//	printf("%d records scanned!\n", scanned);

    std::sort(results.begin(), results.end());  // add by hpy
    return scanned;
}

void pactree_wrapper::get_memory_footprint()  // add by hpy
{
#ifdef MEMORY_FOOTPRINT
    // printf("DRAM Allocated: %llu\n", dram_allocated.load());
    // printf("DRAM Freed: %llu\n", dram_freed.load());
    printf("PMEM Allocated: %llu\n", pmem_allocated.load());
    // printf("PMEM Freed: %llu\n", pmem_freed.load());
#endif
}