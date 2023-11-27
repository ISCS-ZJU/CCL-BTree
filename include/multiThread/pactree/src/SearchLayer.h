// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "numa.h"
#include "../lib/PDL-ART/Tree.h"

class PDLARTIndex {
private:
    Key minKey;
    Key_t curMin;
    pptr<ART_ROWEX::Tree> idxPtr;
    ART_ROWEX::Tree *idx;
    ART_ROWEX::Tree *dummy_idx;
    uint32_t numInserts = 0;
    int numa;
public:
	PDLARTIndex(char* path, size_t size) { //for test..
		if (typeid(Key_t) == typeid(uint64_t)) {
			int is_created;
			root_obj *sl_root;
			PMem::bind(0,path,size,(void **)&sl_root,&is_created);
			PMem::alloc(0,sizeof(ART_ROWEX::Tree),(void **)&idxPtr);

			idx = new(idxPtr.getVaddr()) ART_ROWEX::Tree([] (TID tid,Key &key){
					key.setInt(*reinterpret_cast<uint64_t*>(tid));
					});
			// 
			minKey.setInt(0);
			curMin = ULLONG_MAX;  //add by zhenxin
        		dummy_idx = new ART_ROWEX::Tree([] (TID tid,Key &key){
		             key.setInt(*reinterpret_cast<uint64_t*>(tid));
			});
		}
		else{
#ifdef STRINGKEY
			printf("here for string\n");
			int is_created;
			root_obj *sl_root;
			PMem::bind(0,path,size,(void **)&sl_root,&is_created);
			PMem::alloc(0,sizeof(ART_ROWEX::Tree),(void **)&idxPtr);

			idx = new(idxPtr.getVaddr()) ART_ROWEX::Tree([] (TID tid,Key &key){
	               key.set(reinterpret_cast<char*>(tid), KEYLENGTH);
					});
			std::string maxString= "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
           curMin.setFromString(maxString);
#endif
		}
	}
	PDLARTIndex() {
		if (typeid(Key_t) == typeid(uint64_t)) {
			PMem::alloc(0,sizeof(ART_ROWEX::Tree),(void **)&idxPtr);
			idx = new(idxPtr.getVaddr()) ART_ROWEX::Tree([] (TID tid,Key &key){
				key.setInt(*reinterpret_cast<uint64_t*>(tid));
				});
        		dummy_idx = new ART_ROWEX::Tree([] (TID tid,Key &key){
		             key.setInt(*reinterpret_cast<uint64_t*>(tid));
			});
		}
		else{
			printf("here for string\n");
			PMem::alloc(0,sizeof(ART_ROWEX::Tree),(void **)&idxPtr);

			idx = new(idxPtr.getVaddr()) ART_ROWEX::Tree([] (TID tid,Key &key){
	               key.set(reinterpret_cast<char*>(tid), KEYLENGTH);
					});
				
		}
	}

	~PDLARTIndex() {
		//delete idx;
	}
    void init(){
        pptr<ART_ROWEX::Tree> dummyidxPtr;
        idx = (ART_ROWEX::Tree *)(idxPtr.getVaddr()); 
        idx->genId++;
        dummy_idx = new ART_ROWEX::Tree([] (TID tid,Key &key){
             key.setInt(*reinterpret_cast<uint64_t*>(tid));
	});

    }
    void setNuma(int numa){this->numa=numa;}
    void setKey(Key& k, uint64_t key) {k.setInt(key);}
	void setKey(Key& k, StringKey<KEYLENGTH> key) {k.set(key.getData(), KEYLENGTH);}
    bool insert(Key_t key, void *ptr) {
        auto t = dummy_idx->getThreadInfo();
        Key k;
        setKey(k, key);
        idx->insert(k, (unsigned long)ptr, t);
        if (key < curMin)
            curMin = key;
        numInserts++;
        return true;
    }
    bool remove(Key_t key, void *ptr) {
        auto t = dummy_idx->getThreadInfo();
        Key k;
        setKey(k, key);
        idx->remove(k, (unsigned long)ptr, t);
        numInserts--;
        return true;
    }
#ifdef SYNC
    bool insert(Key_t key, void* ptr, void *locked_node) {
	    bool ret;
	    auto t = idx->getThreadInfo();
	    Key k;
	    setKey(k, key);
	    ret = idx->insert(k, reinterpret_cast<uint64_t>(ptr), t,locked_node);
	    numInserts++;
	    if (key < curMin) curMin = key;
	    return ret;
    }

    void* lookupwithLock(Key_t key, void** node) {
	    if (key <= curMin)
		    return nullptr;
	    auto t = idx->getThreadInfo();
	    Key endKey;
	    setKey(endKey, key);

	    auto result = idx->lookupNextwithLock(endKey, node, t);
	    return reinterpret_cast<void*>(result);
    }

    bool nodeUnlock(void *node){
	    auto t = idx->getThreadInfo();

	    auto result = idx->nodeUnlock(node, t);
	    return result;
    }
#endif


    //Gets the value of the key if present or the value of key just less than/greater than key
    void* lookup(Key_t key) {
        if (key <= curMin)
            return nullptr;
        auto t = dummy_idx->getThreadInfo();
        Key endKey;
        setKey(endKey, key);

        auto result = idx->lookupNext(endKey, t);
        return reinterpret_cast<void*>(result);
    }
    void* lookup2(Key_t key) {
        if (key <= curMin){
            return nullptr;
	}
        auto t = dummy_idx->getThreadInfo();
        Key endKey;
        setKey(endKey, key);

        auto result = idx->lookup(endKey, t);
        return reinterpret_cast<void*>(result);
    }


    // Art segfaults if range operation is done when there are less than 2 keys
    bool isEmpty() {
	    return (numInserts < 2);
    }
    uint32_t size() {return numInserts;}

};
//typedef SortedArray SearchLayer;
typedef PDLARTIndex SearchLayer;
