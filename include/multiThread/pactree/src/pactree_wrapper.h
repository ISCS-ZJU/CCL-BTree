#pragma once

#ifndef PACTREE_WRAPPER_H
#define PACTREE_WRAPPER_H


#include "tree_api.hpp"
#include "pactree.h"
#include <numa-config.h>
#include "pactreeImpl.h"

#include <cstring>
#include <mutex>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <omp.h>


class pactree_wrapper : public tree_api
{
public:
    pactree_wrapper();
    virtual ~pactree_wrapper();

    virtual bool find(const char* key, size_t key_sz, char* value_out) override;
    virtual bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool remove(const char* key, size_t key_sz) override;
    virtual int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;

public:
    pactree *tree_ = nullptr;
    thread_local static bool thread_init;
    void get_memory_footprint();
};
//static std::atomic<uint64_t> i_(0);

#endif