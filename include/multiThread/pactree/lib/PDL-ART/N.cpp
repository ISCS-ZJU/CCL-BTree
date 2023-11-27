#include <assert.h>
#include <algorithm>
#include <iostream>
#include <new>

#include "N.h"
#include "N4.cpp"
#include "N16.cpp"
#include "N48.cpp"
#include "N256.cpp"

namespace ART_ROWEX {
    void N::setType(NTypes type) {
        typeVersionLockObsolete.fetch_add(convertTypeToVersion(type));
//        smp_faa(&(pLock.lock),convertTypeToVersion(type));
    }

    uint64_t N::convertTypeToVersion(NTypes type) {
        return (static_cast<uint64_t>(type) << 62);
    }

    NTypes N::getType() const {
//        return static_cast<NTypes>((pLock.lock) >> 62);
        return static_cast<NTypes>(typeVersionLockObsolete.load(std::memory_order_relaxed) >> 62);
    }

    uint32_t N::getLevel() const {
        return level;
    }

    void N::setLevel(uint32_t lev){
        level=lev;
    }

    void N::writeLockOrRestart(bool &needRestart,uint32_t genId) {
        uint64_t version = typeVersionLockObsolete.load(std::memory_order_relaxed);
	uint64_t new_ver=0;
	uint64_t tmp=0;
        uint32_t lockGenId;
        uint32_t verLock;

	NTypes type = static_cast<NTypes>(version >> 62);
	bool isLeaf = (reinterpret_cast<uint64_t>(version) & (static_cast<uint64_t>(1) << 63))
 == (static_cast<uint64_t>(1) << 63);

	tmp+=(uint64_t)((uint64_t)type<<62);	
	tmp+=(uint64_t)(1<<63);

	lockGenId = (version - tmp)>>32;
	verLock = (uint32_t)(version-tmp);
	new_ver += tmp;
	new_ver += ((uint64_t)genId<<32);
	new_ver += 0b10;

	if(lockGenId != genId){
        	if(!typeVersionLockObsolete.compare_exchange_weak(version, new_ver)){
                	needRestart = true;
			return;
		}
	}
	else{
            do{
            	version = typeVersionLockObsolete.load();
                while (isLocked(version)) {
			_mm_pause();
                	version = typeVersionLockObsolete.load();
	        }
                if (isObsolete(version)) {
			needRestart = true;
			return;
                }
	    } while (!typeVersionLockObsolete.compare_exchange_weak(version, version + 0b10));
	}
#if 0
        do {
            version = typeVersionLockObsolete.load();
            while (isLocked(version)) {
                _mm_pause();
                version = typeVersionLockObsolete.load();
            }
            if (isObsolete(version)) {
                needRestart = true;
                return;
            }
        } while (!typeVersionLockObsolete.compare_exchange_weak(version, version + 0b10));
#endif
    }

    void N::lockVersionOrRestart(uint64_t &version, bool &needRestart, uint32_t genId) {
	uint64_t new_ver=0;
	uint64_t tmp=0;
        uint32_t lockGenId;
        uint32_t verLock;

	NTypes type = static_cast<NTypes>(version >> 62);
	bool isLeaf = (reinterpret_cast<uint64_t>(version) & (static_cast<uint64_t>(1) << 63))
 == (static_cast<uint64_t>(1) << 63);

	tmp+=(uint64_t)((uint64_t)type<<62);	
	tmp+=(uint64_t)(1<<63);

	lockGenId = (version - tmp)>>32;
	verLock = (uint32_t)(version-tmp);
	new_ver += tmp;
	new_ver += ((uint64_t)genId<<32);
	new_ver += 0b10;




        if (isLocked(version) || isObsolete(version)) {
            needRestart = true;
            return;
        }
	if(lockGenId != genId){
		if(!typeVersionLockObsolete.compare_exchange_weak(version, new_ver)){
                	needRestart = true;
			return;
		}
	}
	else{
	        if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
                	version = version + 0b10;
		}
		else {
			needRestart = true;
		}
	}

/*        if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
            version = version + 0b10;
        } else {
            needRestart = true;
        }*/
    }

    void N::writeUnlock() {
        typeVersionLockObsolete.fetch_add(0b10);
    }

    N *N::getAnyChild(const N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getAnyChild();
            }
        }
        assert(false);
        __builtin_unreachable();
    }
    
    N *N::getAnyChildReverse(const N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getAnyChildReverse();
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getAnyChildReverse();
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getAnyChildReverse();
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getAnyChildReverse();
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    void N::change(N *node, uint8_t key, pptr<N> val) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->change(key, val);
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename biggerN>
    void N::insertGrow(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo, bool &needRestart, OpStruct *oplog,uint32_t genId) {
        if (n->insert(key, val)) {
            n->writeUnlock();
            return;
        }

#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif
//FIX ME
/*        pptr<OpStruct> ologPtr;
	PMEMoid oid;
	PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
	OpStruct *olog = ologPtr.getVaddr();*/
        pptr<N> nBigPtr;
        unsigned long poolAddr = (unsigned long)pmemobj_pool_by_ptr(parentNode);
        uint16_t id = -1;
        unsigned long off= (unsigned long)parentNode - poolAddr;
        for(int i=0; i<2; i++){
	   if((unsigned long)PMem::getBaseOf(i*3) == poolAddr){
                id = i;
		break;
           }
        }
        pptr<N> parentPtr(id,off);
        oplog->op = OpStruct::insert;
        oplog->oldNodePtr = (void*)parentPtr.getRawPtr();;
 
	PMem::alloc(poolId,sizeof(biggerN),(void **)&nBigPtr, &(oplog->newNodeOid));
	biggerN *nBig = (biggerN *)new(nBigPtr.getVaddr()) biggerN(n->getLevel(),n->getPrefi());

        n->copyTo(nBig);
        nBig->insert(key, val);
	flushToNVM((char*)nBig, sizeof(biggerN));

        parentNode->writeLockOrRestart(needRestart,genId);
        if (needRestart) {
            PMem::free((void *)nBigPtr.getRawPtr());
            n->writeUnlock();
            return;
        }

        N::change(parentNode, keyParent, nBigPtr);
        oplog->op = OpStruct::done;
        parentNode->writeUnlock();

        n->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(n, threadInfo);
    }

    template<typename curN>
    void N::insertCompact(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo, bool &needRestart, OpStruct* oplog, uint32_t genId) {

#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif
        pptr<N> nNewPtr;
/*        pptr<OpStruct> ologPtr;
		PMEMoid oid;
		PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
		OpStruct *olog = ologPtr.getVaddr();*/

        unsigned long poolAddr = (unsigned long)pmemobj_pool_by_ptr(parentNode);
        uint16_t id = -1;
        unsigned long off= (unsigned long)parentNode - poolAddr;
        for(int i=0; i<2; i++){
	   if((unsigned long)PMem::getBaseOf(i*3) == poolAddr){
                id = i;
		break;
           }
        }
        pptr<N> parentPtr(id,off);
        oplog->op = OpStruct::insert;
        oplog->oldNodePtr = (void*)parentPtr.getRawPtr();;
 
        PMem::alloc(poolId,sizeof(curN),(void **)&nNewPtr,&(oplog->newNodeOid));
	curN *nNew = (curN *)new(nNewPtr.getVaddr()) curN(n->getLevel(),n->getPrefi());

        n->copyTo(nNew);
        nNew->insert(key, val);
	flushToNVM((char*)nNew,sizeof(curN));

        parentNode->writeLockOrRestart(needRestart,genId);
        if (needRestart) {
            PMem::free((void *)nNewPtr.getRawPtr());
            n->writeUnlock();
            return;
        }

        N::change(parentNode, keyParent, nNewPtr);
        oplog->op = OpStruct::done;
        parentNode->writeUnlock();

        n->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(n, threadInfo);
    }

    void N::insertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo, bool &needRestart, OpStruct* oplog, uint32_t genId) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
		uint32_t cValues = n->countValues;
		uint16_t nCompactCount = (uint16_t)(cValues>>16);
		uint16_t nCount = (uint16_t)cValues;
                if (nCompactCount == 4 && nCount <= 3) {
                //if (n->compactCount == 4 && n->count <= 3) {
                    insertCompact<N4>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                    break;
                }
                insertGrow<N4, N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
		uint32_t cValues = n->countValues;
		uint16_t nCompactCount = (uint16_t)cValues>>16;
		uint16_t nCount = (uint16_t)cValues;
                if (nCompactCount == 16 && nCount <= 14) {
                //if (n->compactCount == 16 && n->count <= 14) {
                    insertCompact<N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                    break;
                }
                insertGrow<N16, N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
		uint32_t cValues = n->countValues;
		uint16_t nCompactCount = (uint16_t)cValues>>16;
		uint16_t nCount = (uint16_t)cValues;
                if (nCompactCount == 48 && nCount != 48) {
                //if (n->compactCount == 48 && n->count != 48) {
                    insertCompact<N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                    break;
                }
                insertGrow<N48, N256>(n, parentNode, keyParent, key, val, threadInfo, needRestart,oplog, genId);
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->insert(key, val);
                node->writeUnlock();
                break;
            }
        }
    }

    pptr<N> N::getChildPptr(const uint8_t k, N *node){
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getChildPptr(k);
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                return n->getChildPptr(k);
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                return n->getChildPptr(k);
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                return n->getChildPptr(k);
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    N *N::getChild(const uint8_t k, N *node){
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getChild(k);
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                return n->getChild(k);
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                return n->getChild(k);
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                return n->getChild(k);
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    void N::deleteChildren(N *node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->deleteChildren();
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename smallerN>
    void N::removeAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, ThreadInfo &threadInfo, bool &needRestart, OpStruct* oplog, uint32_t genId) {
        if (n->remove(key, parentNode == nullptr)) {
            n->writeUnlock();
            return;
        }

        //auto nSmall = new smallerN(n->getLevel(), n->getPrefi());

#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif

/*        pptr<N> nNewPtr;
        pptr<OpStruct> ologPtr;
		PMEMoid oid;
		PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
		OpStruct *olog = ologPtr.getVaddr();*/
        pptr<N> nSmallPtr;
        unsigned long poolAddr = (unsigned long)pmemobj_pool_by_ptr(parentNode);
        uint16_t id = -1;
        unsigned long off= (unsigned long)parentNode - poolAddr;
        for(int i=0; i<2; i++){
	   if((unsigned long)PMem::getBaseOf(i*3) == poolAddr){
                id = i;
		break;
           }
        }
        pptr<N> parentPtr(id,off);
        
        //pptr<curN> nSmallPtr;
                oplog->op = OpStruct::insert;
                oplog->oldNodePtr = (void*)parentPtr.getRawPtr();;
		PMem::alloc(poolId,sizeof(smallerN),(void **)&nSmallPtr,&(oplog->newNodeOid));
		smallerN *nSmall= (smallerN *)new(nSmallPtr.getVaddr()) smallerN(n->getLevel(),n->getPrefi());

/*	nSmall->eetType(nodeType);
	nSmall->setLevel(n->getLevel());
	Prefix p = n->getPrefi();
	nSmall->setPrefix(p.prefix, p.prefixCount,false);*/

        parentNode->writeLockOrRestart(needRestart,genId);
        if (needRestart) {
            //freeNVM(nSmall);
            n->writeUnlock();
            return;
        }

        n->remove(key, true);
        n->copyTo(nSmall);
	flushToNVM((char*)nSmall,sizeof(smallerN));
        N::change(parentNode, keyParent, nSmallPtr);
        oplog->op = OpStruct::done;

        parentNode->writeUnlock();
        n->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(n, threadInfo);
    }

    void N::removeAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent, ThreadInfo &threadInfo, bool &needRestart, OpStruct* oplog, uint32_t genId) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->remove(key, false);
                n->writeUnlock();
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);

		NTypes nodeType = NTypes::N4;
                removeAndShrink<N16, N4>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
                break;
            }
            case NTypes::N48: {
		NTypes nodeType = NTypes::N16;
                auto n = static_cast<N48 *>(node);
                removeAndShrink<N48, N16>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
                break;
            }
            case NTypes::N256: {
		NTypes nodeType = NTypes::N48;
                auto n = static_cast<N256 *>(node);
                removeAndShrink<N256, N48>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
                break;
            }
        }
    }

    bool N::isLocked(uint64_t version) const {
        return ((version & 0b10) == 0b10);
    }

    uint64_t N::getVersion() const {
        return typeVersionLockObsolete.load();
    }

    bool N::isObsolete(uint64_t version) {
        return (version & 1) == 1;
    }

    bool N::checkOrRestart(uint64_t startRead) const {
        return readUnlockOrRestart(startRead);
    }

    bool N::readUnlockOrRestart(uint64_t startRead) const {
        return startRead == typeVersionLockObsolete.load();
    }

    uint32_t N::getCount() const {
	uint32_t cValues = countValues;
	uint16_t c= (uint16_t)cValues;
        return c;
    }

    Prefix N::getPrefi() const {
        return prefix.load();
    }

    void N::setPrefix(const uint8_t *prefix, uint32_t length, bool flush) {
        if (length > 0) {
            Prefix p;
            memcpy(p.prefix, prefix, std::min(length, maxStoredPrefixLength));
            p.prefixCount = length;
            this->prefix.store(p, std::memory_order_release);
        } else {
            Prefix p;
            p.prefixCount = 0;
            this->prefix.store(p, std::memory_order_release);
        }
	if(flush)
           flushToNVM((char*)&(this->prefix),sizeof(Prefix));
    }

    void N::addPrefixBefore(N* node, uint8_t key) {
        Prefix p = this->getPrefi();
        Prefix nodeP = node->getPrefi();
        uint32_t prefixCopyCount = std::min(maxStoredPrefixLength, nodeP.prefixCount + 1);
        memmove(p.prefix + prefixCopyCount, p.prefix, std::min(p.prefixCount, maxStoredPrefixLength - prefixCopyCount));
        memcpy(p.prefix, nodeP.prefix, std::min(prefixCopyCount, nodeP.prefixCount));
        if (nodeP.prefixCount < maxStoredPrefixLength) {
            p.prefix[prefixCopyCount - 1] = key;
        }
        p.prefixCount += nodeP.prefixCount + 1;
        this->prefix.store(p, std::memory_order_release);
        flushToNVM((char*)&(this->prefix),sizeof(Prefix));
    }

    bool N::isLeaf(const N *n) {
        return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << 63)) == (static_cast<uint64_t>(1) << 63);
    }

    N *N::setLeaf(TID tid) {
        return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << 63));
    }

    TID N::getLeaf(const N *n) {
        return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << 63) - 1));
    }

    std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getSecondChild(key);
            }
            default: {
                assert(false);
                __builtin_unreachable();
            }
        }
    }

    void N::deleteNode(N *node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
		PMem::freeVaddr((void *)n);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
		PMem::freeVaddr((void *)n);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
		PMem::freeVaddr((void *)n);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
		PMem::freeVaddr((void *)n);
                return;
            }
        }
	PMem::freeVaddr(node);
    }

    TID N::getAnyChildTid(const N *n) {
        const N *nextNode = n;

        while (true) {
            const N *node = nextNode;
            nextNode = getAnyChild(node);

            assert(nextNode != nullptr);
            if (isLeaf(nextNode)) {
                return getLeaf(nextNode);
            }
        }
    }
    
    TID N::getAnyChildTidReverse(const N *n) {
        const N *nextNode = n;

        while (true) {
            const N *node = nextNode;
            nextNode = getAnyChildReverse(node);

            assert(nextNode != nullptr);
            if (isLeaf(nextNode)) {
                return getLeaf(nextNode);
            }
        }
    }

    void N::getChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                        uint32_t &childrenCount) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
        }
    }
    N *N::getSmallestChild(const N *node, uint8_t start) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getSmallestChild(start);
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getSmallestChild(start);
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getSmallestChild(start);
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getSmallestChild(start);
            }
        }
        assert(false);
        __builtin_unreachable();
    }
    N *N::getLargestChild(const N *node, uint8_t start) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getLargestChild(start);
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getLargestChild(start);
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getLargestChild(start);
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getLargestChild(start);
            }
        }
        assert(false);
        __builtin_unreachable();
    }
}
