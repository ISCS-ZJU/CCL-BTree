// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef _VERSIONEDLOCK_H
#define _VERSIONEDLOCK_H
#include <atomic>
#include <iostream>
#include "arch.h"

#define PAUSE asm volatile("pause\n": : :"memory");
typedef unsigned long version_t;

class VersionedLock {
private:
    std::atomic<version_t> version;

public:
	VersionedLock() : version(2) {}

	version_t read_lock(uint32_t genId) {
	        version_t ver = version.load(std::memory_order_acquire);
		uint32_t lockGenId;
		uint32_t verLock;

		lockGenId = ver >> 32;
		verLock = (int32_t)ver;

		if(genId != lockGenId){
			// versionLock = 2;
			version_t new_ver = (genId << 32) + 2;
			if(version.compare_exchange_weak(ver, new_ver)){
				return new_ver;	    
			}
			return 0;
		}
		else{
			if ((ver & 1) != 0) {
				return 0;
			}
			return ver;
		}
	}

	version_t write_lock(uint32_t genId) {
	        version_t ver = version.load(std::memory_order_acquire);
		uint32_t lockGenId;
		uint32_t verLock;

		lockGenId = ver >> 32;
		verLock = (int32_t)ver;

		if(genId != lockGenId){
			// versionLock = 2;
			version_t new_ver = (genId << 32) + 3;
			if(version.compare_exchange_weak(ver, new_ver)){
				return new_ver;	    
			}
			return 0;

		}
		else{
			if ((ver & 1) == 0 && version.compare_exchange_weak(ver, ver+1)) {
				return ver;
			}
			return 0;
		}
	}

    bool read_unlock(version_t old_version) {
        std::atomic_thread_fence(std::memory_order_acquire);
        version_t new_version = version.load(std::memory_order_acquire);
        return new_version == old_version;
    }

    void write_unlock() {
        version.fetch_add(1, std::memory_order_release);
        return;
    }
};
#endif //_VERSIONEDLOCK_H
