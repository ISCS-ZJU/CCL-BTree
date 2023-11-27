#include <assert.h>
#include <algorithm>
#include "N.h"
#include <emmintrin.h> // x86 SSE intrinsics

namespace ART_ROWEX {

    bool N16::insert(uint8_t key, pptr<N> n) {
	    return insert(key,n,true);
    }
    bool N16::insert(uint8_t key, pptr<N> n, bool flush) {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        if (compactCount == 16) {
            return false;
        }

	n.markDirty();	 //DL
        children[compactCount].store(n, std::memory_order_release);
	//TODO
	if(flush){
		flushToNVM((char*)&children[compactCount],sizeof(pptr<N>));
		smp_wmb();
	}

        keys[compactCount].store(flipSign(key), std::memory_order_release);
	uint32_t increaseCountValues = (1<<16)+1;
	countValues+=increaseCountValues; // visible point
        //compactCount++;
        //count++;
	if(flush){
		flushToNVM((char*)this,L1_CACHE_BYTES);
		smp_wmb();
	}
	n.markClean();	//DL
        children[compactCount].store(n, std::memory_order_release);
        return true;
    }

    template<class NODE>
    void N16::copyTo(NODE *n) const {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (unsigned i = 0; i < compactCount; i++) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                n->insert(flipSign(keys[i]), child,false);
            }
        }
    }

    void N16::change(uint8_t key, pptr<N> val) {
        auto childPos = getChildPos(key);
        assert(childPos != nullptr);
	val.markDirty();	 //DL
        childPos->store(val, std::memory_order_release);
	flushToNVM((char*)childPos,sizeof(pptr<N>));
	smp_wmb();
	val.markClean();	//DL
	//DL
	childPos->store(val, std::memory_order_release);
    }

    std::atomic<pptr<N>> *N16::getChildPos(const uint8_t k) {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                                     _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
        unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
        while (bitfield) {
            uint8_t pos = ctz(bitfield);
            pptr<N> child = children[pos].load();
	    N *rawChild = child.getVaddr();

            if (rawChild != nullptr) {
                return &children[pos];
            }
            bitfield = bitfield ^ (1 << pos);
        }
        return nullptr;
    }

    pptr<N> N16::getChildPptr(const uint8_t k) const {
        __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                                     _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
        unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
        while (bitfield) {
            uint8_t pos = ctz(bitfield);
            pptr<N> child = children[pos].load();
	    while(child.isDirty()){ //DL
            	 child = children[pos].load();
	    }
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr && keys[pos].load() == flipSign(k)) {
                return child;
            }
            bitfield = bitfield ^ (1 << pos);
        }

	pptr<N> nullPtr(0,0);
        return nullPtr;
    }

    N *N16::getChild(const uint8_t k) const {
        __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                                     _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
        unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
        while (bitfield) {
            uint8_t pos = ctz(bitfield);
            pptr<N> child = children[pos].load();
	    while(child.isDirty()){ //DL
            	 child = children[pos].load();
	    }
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr && keys[pos].load() == flipSign(k)) {
                return rawChild;
            }
            bitfield = bitfield ^ (1 << pos);
        }
        return nullptr;
    }

    bool N16::remove(uint8_t k, bool force) {
	uint16_t count = (uint16_t)countValues;
        if (count == 3 && !force) {
            return false;
        }
        auto leafPlace = getChildPos(k);
        assert(leafPlace != nullptr);
	pptr<N> nullPtr(0,0);
	nullPtr.markDirty();	
        leafPlace->store(nullPtr, std::memory_order_release);
	flushToNVM((char*)leafPlace,sizeof(pptr<N>));
	smp_wmb();
//        count--;
	uint32_t decreaseCountValues = (1<<16);
	countValues-=decreaseCountValues; // visible point
    	flushToNVM((char*)this,L1_CACHE_BYTES);
	smp_wmb();
        assert(getChild(k) == nullptr);
	nullPtr.markClean();	
        leafPlace->store(nullPtr, std::memory_order_release);
        return true;
    }

    N *N16::getAnyChild() const {
        N *anyChild = nullptr;
        for (int i = 0; i < 16; ++i) {
            pptr<N> child = children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = children[i].load();
	    	}
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                if (N::isLeaf(rawChild)) {
                    return rawChild;
                }
                anyChild = rawChild;
            }
        }
        return anyChild;
    }
    
    N *N16::getAnyChildReverse() const {
        N *anyChild = nullptr;
        for (int i = 15; i >= 0; --i) {
            pptr<N> child = children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = children[i].load();
	    	}
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                if (N::isLeaf(rawChild)) {
                    return rawChild;
                }
                anyChild = rawChild;
            }
        }
        return anyChild;
    }

    void N16::deleteChildren() {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (std::size_t i = 0; i < compactCount; ++i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                N::deleteChildren(rawChild);
                N::deleteNode(rawChild);
            }
        }
    }

    void N16::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
        childrenCount = 0;
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (int i = 0; i < compactCount; ++i) {
            uint8_t key = flipSign(this->keys[i]);
            if (key >= start && key <= end) {
                pptr<N> child = this->children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = this->children[i].load();
	    	}
                N *rawChild = child.getVaddr();
                if (rawChild != nullptr) {
                    children[childrenCount] = std::make_tuple(key, rawChild);
                    childrenCount++;
                }
            }
        }
        std::sort(children, children + childrenCount, [](auto &first, auto &second) {
            return std::get<0>(first) < std::get<0>(second);
        });
    }
    N *N16::getSmallestChild(uint8_t start) const {
        N *smallestChild = nullptr;
        uint8_t minKey = 255;
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (int i = 0; i < compactCount; ++i) {
            uint8_t key = flipSign(this->keys[i]);
            if (key >= start && key <= minKey) {
                pptr<N> child = this->children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = this->children[i].load();
	    	}
                N *rawChild = child.getVaddr();
                if (rawChild != nullptr) {
                    minKey = key;
                    smallestChild = rawChild;
                }
            }
        }
        return smallestChild;
    }
    N *N16::getLargestChild(uint8_t end) const {
        N *largestChild = nullptr;
        uint8_t maxKey = 0;
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (int i = 0; i < compactCount; ++i) {
            uint8_t key = flipSign(this->keys[i]);
            if (key <= end && key >= maxKey) {
                pptr<N> child = this->children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = children[i].load();
	    	}
                N *rawChild = child.getVaddr();
                if (rawChild != nullptr) {
                    maxKey = key;
                    largestChild = rawChild;
                }
            }
        }
        return largestChild;
    }
}
