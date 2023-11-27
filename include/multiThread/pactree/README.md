# PACTree PiBench Wrapper

PiBench wrapper for PACTree.

### 1. Installation
Install packages if necessary
sudo apt-get install g++ libtbb-dev libjemalloc-dev libnuma-dev libpmem-dev libpmemobj-dev python zlib1g-dev libboost-dev 

Configure macros in include/common.h

Configure pool paths and sizes in src/pactree.cpp (function initPT)

```
$ mkdir build && cmake .. && make
```


### 3. Notes
PACTree code from https://github.com/cosmoss-vt/pactree (commit `f173a0f30a3ea492cdc194e6e83a00f9807a8cc8`)
