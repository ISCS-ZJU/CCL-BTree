#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_ROWEX {


    void N4::deleteChildren() {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (uint32_t i = 0; i < compactCount; ++i) {
            pptr<N> child = children[i].load();
	    N* rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                N::deleteChildren(rawChild);
                N::deleteNode(rawChild);
            }
        }
    }

    bool N4::insert(uint8_t key, pptr<N> n) {
	return insert(key,n,true);
    }

    bool N4::insert(uint8_t key, pptr<N> n, bool flush) {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        if (compactCount == 4) {
            return false;
        }
        keys[compactCount].store(key, std::memory_order_release);
	n.markDirty();	
        children[compactCount].store(n, std::memory_order_release); //visible point for insert function
//	idxPtr childPtr = children[compactCount].load();
	uint32_t increaseCountValues = (1<<16)+1;
	countValues+=increaseCountValues; // visible point
	if(flush){ 
		flushToNVM((char*)this, sizeof(N4));
		smp_wmb();
	}
	n.markClean();	
        children[compactCount].store(n, std::memory_order_release); //visible point for insert function

        //compactCount++;
        //count++;
        return true;
    }

    template<class NODE>
    void N4::copyTo(NODE *n) const {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (uint32_t i = 0; i < compactCount; ++i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                n->insert(keys[i].load(), child, false); //no flush
            }
        }
    }

    void N4::change(uint8_t key, pptr<N> val) {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (uint32_t i = 0; i < compactCount; ++i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr && keys[i].load() == key) {
		//DL
		val.markDirty();	
                children[i].store(val, std::memory_order_release);
	    	flushToNVM((char*)&children[i],sizeof(pptr<N>));
		smp_wmb();
		//DL
		val.markClean();	
		//DL
		children[i].store(val, std::memory_order_release);
		return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }
    pptr<N> N4::getChildPptr(const uint8_t k) const {
        for (uint32_t i = 0; i < 4; ++i) {
            pptr<N> child = children[i].load();
            while(child.isDirty()){ //DL
            	 child = children[i].load();
	    }
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr && keys[i].load() == k) {
                return child;
            }
        }
	pptr<N> nullPtr(0,0);
        return nullPtr;
    }

    N *N4::getChild(const uint8_t k) const {
        for (uint32_t i = 0; i < 4; ++i) {
            pptr<N> child = children[i].load();
            while(child.isDirty()){ //DL
            	 child = children[i].load();
	    }
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr && keys[i].load() == k) {
                return rawChild;
            }
        }
        return nullptr;
    }

    bool N4::remove(uint8_t k, bool /*force*/) {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (uint32_t i = 0; i < compactCount; ++i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr && keys[i].load() == k) {
		uint32_t decreaseCountValues = (1<<16);
		countValues-=decreaseCountValues; // visible point
		pptr<N> nullPtr(0,0);
		nullPtr.markDirty();	
                children[i].store(nullPtr, std::memory_order_release);
	    	flushToNVM((char*)&children[i],sizeof(pptr<N>));
		smp_wmb();
		nullPtr.markClean();	
                children[i].store(nullPtr, std::memory_order_release);
                return true;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    N *N4::getAnyChild() const {
        N *anyChild = nullptr;
        for (uint32_t i = 0; i < 4; ++i) {
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
    
    N *N4::getAnyChildReverse() const {
        N *anyChild = nullptr;
        for (int i = 3; i >= 0; --i) {
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

    std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        for (uint32_t i = 0; i < compactCount; ++i) {
            pptr<N> child = children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = children[i].load();
	    	}
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                uint8_t k = keys[i].load();
                if (k != key){
                    return std::make_tuple(rawChild, k);
                }
            }
        }
        return std::make_tuple(nullptr, 0);
    }

    void N4::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                         uint32_t &childrenCount) const {
        childrenCount = 0;
        for (uint32_t i = 0; i < 4; ++i) {
            uint8_t key = this->keys[i].load();
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
    N *N4::getSmallestChild(uint8_t start) const {
        N *smallestChild = nullptr;
        uint8_t minKey = 255;
        for (uint32_t i = 0; i < 4; ++i) {
            uint8_t key = this->keys[i].load();
            if (key >= start && key <= minKey) {
                pptr<N> child = children[i].load();
	    	while(child.isDirty()){ //DL
            		 child = children[i].load();
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
    N *N4::getLargestChild(uint8_t end) const {
        N *largestChild = nullptr;
        uint8_t maxKey = 0;
        for (uint32_t i = 0; i < 4; ++i) {
            uint8_t key = this->keys[i].load();
            if (key <= end && key >= maxKey) {
                pptr<N> child = children[i].load();
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
