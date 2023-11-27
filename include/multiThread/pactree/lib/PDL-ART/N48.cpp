#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_ROWEX {

    bool N48::insert(uint8_t key, pptr<N> n) {
	    return insert(key,n,true);
    }
    bool N48::insert(uint8_t key, pptr<N> n, bool flush) {
	uint16_t compactCount = (uint16_t)(countValues>>16);
        if (compactCount == 48) {
            return false;
        }
	n.markDirty();	 //DL
        children[compactCount].store(n, std::memory_order_release);
	if(flush) flushToNVM((char*)&children[compactCount],sizeof(pptr<N>));
        childIndex[key].store(compactCount, std::memory_order_release);
	if(flush){
		flushToNVM((char*)&childIndex[key],sizeof(uint8_t));
		smp_wmb();
    	}
	uint32_t increaseCountValues = (1<<16)+1;
	countValues+=increaseCountValues; // visible point
	if(flush){
		flushToNVM((char*)this,L1_CACHE_BYTES);
		smp_wmb();
	}
	n.markClean();	//DL
        children[compactCount].store(n, std::memory_order_release);
        //compactCount++;
        //count++;
        return true;
    }

    template<class NODE>
    void N48::copyTo(NODE *n) const {
        for (unsigned i = 0; i < 256; i++) {
            uint8_t index = childIndex[i].load();
            if (index != emptyMarker) {
                n->insert(i, children[index],false);
            }
        }
    }

    void N48::change(uint8_t key, pptr<N> val) {
        uint8_t index = childIndex[key].load();
        assert(index != emptyMarker);
	val.markDirty();	 //DL 
        children[index].store(val, std::memory_order_release);
	flushToNVM((char*)&children[index],sizeof(pptr<N>));
	smp_wmb();
	val.markClean();	//DL
	children[index].store(val, std::memory_order_release);
	return;
    }

    pptr<N> N48::getChildPptr(const uint8_t k) const {
        uint8_t index = childIndex[k].load();
        if (index == emptyMarker) {
	    pptr<N> nullPtr(0,0);
            return nullPtr;
        } else {
            pptr<N> child = children[index].load();
	    while(child.isDirty()){ //DL
            	 child = children[index].load();
	    }
            return child; 
            //return children[index].load();
        }
    }



    N *N48::getChild(const uint8_t k) const {
        uint8_t index = childIndex[k].load();
        if (index == emptyMarker) {
            return nullptr;
        } else {
            pptr<N> child = children[index].load();
	    while(child.isDirty()){ //DL
            	 child = children[index].load();
	    }
            N *rawChild = child.getVaddr();
            return rawChild; 
            //return children[index].load();
        }
    }

    bool N48::remove(uint8_t k, bool force) {
	uint16_t count = (uint16_t)countValues;
        if (count == 12 && !force) {
            return false;
        }
        assert(childIndex[k] != emptyMarker);
	pptr<N> nullPtr(0,0);
	nullPtr.markDirty();	
        children[childIndex[k]].store(nullPtr, std::memory_order_release);
	flushToNVM((char*)&children[childIndex[k]],sizeof(pptr<N>));
        childIndex[k].store(emptyMarker, std::memory_order_release);
	flushToNVM((char*)&childIndex[k],sizeof(uint8_t));
	smp_wmb();
        //count--;
	uint32_t decreaseCountValues = 1<<16;
	countValues-=decreaseCountValues; // visible point
	flushToNVM((char*)this,L1_CACHE_BYTES);
	smp_wmb();
        assert(getChild(k) == nullptr);
	nullPtr.markClean();	
        children[childIndex[k]].store(nullPtr, std::memory_order_release);
        return true;
    }

    N *N48::getAnyChild() const {
        N *anyChild = nullptr;
        for (unsigned i = 0; i < 48; i++) {
            pptr<N> child = children[i].load();
	    while (child.isDirty ()) {  // DL
                child = children[i].load ();
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

    N *N48::getAnyChildReverse() const {
        N *anyChild = nullptr;
        for (int i = 47; i >= 0; i--) {
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

    void N48::deleteChildren() {
        for (unsigned i = 0; i < 256; i++) {
            if (childIndex[i] != emptyMarker) {
                pptr<N> child = children[i].load();
                N *rawChild = child.getVaddr();
                N::deleteChildren(rawChild);
                N::deleteNode(rawChild);
            }
        }
    }

    void N48::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
        childrenCount = 0;
        for (unsigned i = start; i <= end; i++) {
            uint8_t index = this->childIndex[i].load();
            if (index != emptyMarker) {
                pptr<N> child = this->children[index].load();
	    	while(child.isDirty()){ //DL
            		 child = this->children[index].load();
	    	}
                N *rawChild = child.getVaddr();
                if (rawChild != nullptr) {
                    children[childrenCount] = std::make_tuple(i, rawChild);
                    childrenCount++;
                }
            }
        }
    }

    N *N48::getSmallestChild(uint8_t start) const {
        N *smallestChild = nullptr;
        for (int i = start; i < 256; ++i) {
            uint8_t index = this->childIndex[i].load();
            if (index != emptyMarker) {
                pptr<N> child = children[index].load();
	    	while(child.isDirty()){ //DL
            		 child = children[index].load();
	    	}
                N *rawChild = child.getVaddr();
                if (rawChild != nullptr) {
                        return rawChild;
                }
            }
        }
        return smallestChild;
    }
    N *N48::getLargestChild(uint8_t end) const {
        N *largestChild = nullptr;
        for (int i = end; i >= 0; --i) {
            uint8_t index = this->childIndex[i].load();
            if (index != emptyMarker) {
                pptr<N> child = children[index].load();
	    	while(child.isDirty()){ //DL
            		 child = children[index].load();
	    	}
                N *rawChild = child.getVaddr();
                if (rawChild != nullptr) {
                        return rawChild;
                }
            }
        }
        return largestChild;
    }
}
