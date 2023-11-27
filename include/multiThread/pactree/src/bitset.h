// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef pactree_BITSET_H
#define pactree_BITSET_H

#include <cstdint>
#include <assert.h>

namespace hydra {
    class bitset {
    private:
        uint64_t bits[2];
        bool testBit(int pos, uint64_t &bits) {
            return (bits & (1UL << (pos))) != 0;
        }
        void setBit(int pos, uint64_t &bits) {
            bits |= 1UL << pos;
        }
        void resetBit(int pos, uint64_t &bits) {
            bits &= ~(1UL << pos);
        }
    public:
        void clear() {
            bits[0] = 0;
            bits[1] = 0;
        }
        void set(int index) {
            //assert(index < 128 && index >= 0);
            setBit(index, bits[index/64]);
        }
        void reset(int index) {
            //assert(index < 128 && index >= 0);
            resetBit(index, bits[index/64]);
        }
        bool test(int index) {
            //assert(index < 128 && index >= 0);
            return testBit(index, bits[index/64]);
        }
        bool operator[] (int index) {
            return test(index);
        }
        uint64_t to_ulong() {
            return bits[0];
        }
        uint64_t to_ulong(int index) {
            return bits[index];
        }

    };
};

#endif 
