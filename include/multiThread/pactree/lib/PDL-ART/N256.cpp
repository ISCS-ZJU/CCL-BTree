#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_ROWEX {

    void N256::deleteChildren() {
        for (uint64_t i = 0; i < 256; ++i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                N::deleteChildren(rawChild);
                N::deleteNode(rawChild);
            }
        }
    }

    bool N256::insert(uint8_t key, pptr<N> val) {
	    return insert(key,val,true);
    }
    bool N256::insert(uint8_t key, pptr<N> val,bool flush) {
	//uint16_t count = (uint16_t)countValues;
	val.markDirty();	 //DL
        children[key].store(val, std::memory_order_release);
	if(flush){
		flushToNVM((char*)&children[key],sizeof(pptr<N>));
		smp_wmb();
	}
        //count++;
	uint32_t increaseCountValues = (1<<16);
	countValues+=increaseCountValues; // visible point
	if(flush){
		flushToNVM((char*)this,L1_CACHE_BYTES);
		smp_wmb();
	}
	val.markClean();	//DL
        children[key].store(val, std::memory_order_release);
        return true;
    }

    template<class NODE>
    void N256::copyTo(NODE *n) const {
        for (int i = 0; i < 256; ++i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                n->insert(i, child,false);
            }
        }
    }

    void N256::change(uint8_t key, pptr<N> n) {
	n.markDirty();	 //DL
        children[key].store(n, std::memory_order_release);
	flushToNVM((char*)&children[key],sizeof(pptr<N>));
	smp_wmb();
	n.markClean();	//DL
        children[key].store(n, std::memory_order_release);
    }

    pptr<N> N256::getChildPptr(const uint8_t k) const {
        pptr<N> child = children[k].load();
	while(child.isDirty()){ //DL
        	child = children[k].load();
	}
        return child;
    }

    N *N256::getChild(const uint8_t k) const {
        pptr<N> child = children[k].load();
	while(child.isDirty()){ //DL
        	child = children[k].load();
	}
        N *rawChild = child.getVaddr();
        return rawChild;
    }

    bool N256::remove(uint8_t k, bool force) {
	uint16_t count = (uint16_t)countValues;
        if (count == 37 && !force) {
            return false;
        }
	pptr<N> nullPtr(0,0);
	nullPtr.markDirty();	 //DL
        children[k].store(nullPtr, std::memory_order_release);
	flushToNVM((char*)&children[k],sizeof(pptr<N>));
	smp_wmb();
        //count--;
	uint32_t decreaseCountValues = (1<<16);
	countValues-=decreaseCountValues; // visible point
	flushToNVM((char*)this,L1_CACHE_BYTES);
	smp_wmb();
	nullPtr.markClean();	//DL
        children[k].store(nullPtr, std::memory_order_release);
        return true;
    }

    N *N256::getAnyChild() const {
        N *anyChild = nullptr;
        for (uint64_t i = 0; i < 256; ++i) {
            pptr<N> child = children[i].load();
	    while (child.isDirty ()) {  // DL
                child = children[i].load ();
            }
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                if (N::isLeaf(rawChild)) {
                    return rawChild;
                } else {
                    anyChild = rawChild;
                }
            }
        }
        return anyChild;
    }
    
    N *N256::getAnyChildReverse() const {
        N *anyChild = nullptr;
        for (int i = 255; i >= 0; --i) {
            pptr<N> child = children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                if (N::isLeaf(rawChild)) {
                    return rawChild;
                } else {
                    anyChild = rawChild;
                }
            }
        }
        return anyChild;
    }

    void N256::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                           uint32_t &childrenCount) const {
        childrenCount = 0;
        for (unsigned i = start; i <= end; i++) {
            pptr<N> child = this->children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                children[childrenCount] = std::make_tuple(i, rawChild);
                childrenCount++;
            }
        }
    }

    N *N256::getSmallestChild(uint8_t start) const {
        N *smallestChild = nullptr;
        for (int i = start; i < 256; ++i) {
            pptr<N> child = this->children[i].load();
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                return rawChild;
            }
        }
        return smallestChild;
    }
    N *N256::getLargestChild(uint8_t end) const {
        N *largestChild = nullptr;
        for (int i = end; i >= 0; --i) {
            pptr<N> child = this->children[i].load();
    	while(child.isDirty()){ //DL
           		 child = children[i].load();
    	}
	    N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                return rawChild;
            }
        }
        return largestChild;
    }
}
