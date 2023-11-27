#include <assert.h>
#include <algorithm>
#include <functional>
#include "Tree.h"
#include "N.cpp"
#include "Epoche.cpp"
#include "Key.h"


namespace ART_ROWEX {
        void Tree::recover(){
            for(int i=0; i<oplogsCount; i++){
		if(oplogs[i].op == OpStruct::insert){
			pmemobj_free(&oplogs[i].newNodeOid);
                }
            }
        }

	Tree::Tree(LoadKeyFunction loadKey):loadKey(loadKey){

        pptr<OpStruct> ologPtr;
		PMEMoid oid;
		PMem::alloc(0,sizeof(OpStruct),(void **)&ologPtr, &oid);
		OpStruct *olog = ologPtr.getVaddr();
		pptr<N> nRootPtr;
		//PMem::alloc(0,sizeof(N256),(void **)&nRootPtr);
		PMem::alloc(0,sizeof(N256),(void **)&nRootPtr, &(olog->newNodeOid));
		//PMem::alloc(0,sizeof(N256),(void **)&nRootPtr, &(oplogs[oplogsCount].newNodeOid));
		oplogsCount=1;
		flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
		smp_wmb();

		//printf("Tree Constructor\n");
		genId = 3;
		N256 *rootRawPtr= (N256*)new(nRootPtr.getVaddr()) N256(0,{});

		flushToNVM((char *)rootRawPtr,sizeof(N256));
		smp_wmb();
		this->loadKey = loadKey;
		root = nRootPtr;
		//	mtable.initialize();
		flushToNVM((char *)this,sizeof(Tree));
		smp_wmb();


	}

    Tree::~Tree() {
        //N::deleteChildren(root);
        //N::deleteNode(root);
    }

    ThreadInfo Tree::getThreadInfo() {
        return ThreadInfo(this->epoche);
    }

    TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo) const {
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        pptr<N> nodePtr = root;
	N* node = (N*)nodePtr.getVaddr();
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;

        while (true) {
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match: {
                    if (k.getKeyLen() <= level) {
                        return 0;
                    }
                    node = N::getChild(k[level], node);

                    if (node == nullptr) {
                        return 0;
                    }
                    if (N::isLeaf(node)) {
                        TID tid = N::getLeaf(node);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
                            return checkKey(tid, k);
                        } else {
                            return tid;
                        }
                    }
                }
            }
            level++;
        }
    }


    TID Tree::checkKey(const TID tid, const Key &k) const {
        Key kt;
        this->loadKey(tid, kt);

        if (k == kt) {
            return tid;
        }
        return 0;
    }

#ifdef SYNC
    bool Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo, void *locked_node) {
        EpocheGuard epocheGuard(epocheInfo);
        restart:
        bool needRestart = false;
        pptr<N> nextNodePtr = root;

        N *node = nullptr;
        N *nextNode = (N*)root.getVaddr();
        N *parentNode = nullptr;
        pptr<N> nodePtr;
        pptr<N> parentPtr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;
		uint64_t gId = genId;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            parentPtr = nodePtr;
            node = nextNode;
            nodePtr = nextNodePtr;	
            auto v = node->getVersion();

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                                           this->loadKey)) { // increases level
                case CheckPrefixPessimisticResult::SkippedLevel:
                    goto restart;
                case CheckPrefixPessimisticResult::NoMatch: {
                    assert(nextLevel < k.getKeyLen()); //prevent duplicate key
                //    if((node != locked_node) && (locked_node != nullptr)){
		//	printf("t :%lu 1 ART::INSERT node :%p\n",pthread_self(),node);
	                node->lockVersionOrRestart(v, needRestart,gId);
                        if (needRestart) goto restart;
		 //   }

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    Prefix prefi = node->getPrefi();
                    prefi.prefixCount = nextLevel - level;
                    //auto newNode = new N4(nextLevel, prefi);

#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif


                    /* pptr<OpStruct> ologPtr;
                     PMEMoid oid;
                     PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
                     OpStruct *olog = ologPtr.getVaddr();*/
                     pptr<N> newNodePtr;
                     //PMem::alloc(poolId,sizeof(N4),(void **)&newNodePtr, &(olog->newNodeOid));
                     oplogs[oplogsCount].op = OpStruct::insert;
                     oplogs[oplogsCount].oldNodePtr = (void*)parentPtr.getRawPtr();;
                     PMem::alloc(poolId,sizeof(N4),(void **)&newNodePtr, &(oplogs[oplogsCount].newNodeOid));
		     
                     oplogsCount++;
                     flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
	             smp_wmb();
                     N4 *newNode= (N4*)new(newNodePtr.getVaddr()) N4(nextLevel,prefi);

		     pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));


                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], pTid,false);
                    newNode->insert(nonMatchingKey, nodePtr,false);
				    flushToNVM((char*)newNode,sizeof(N4));
				    smp_wmb();

                    // 3) lockVersionOrRestart, update parentNode to point to the new node, unlock
                    parentNode->writeLockOrRestart(needRestart, genId);
                    if (needRestart) {
                    //    delete newNode;
						PMem::free((void *)newNodePtr.getRawPtr());
                        node->writeUnlock();
                        goto restart;
                    }
                    N::change(parentNode, parentKey, newNodePtr);
                    oplogs[oplogsCount].op = OpStruct::done;
                    parentNode->writeUnlock();

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix.prefix,
                                    node->getPrefi().prefixCount - ((nextLevel - level) + 1),true);

                    node->writeUnlock();

		        	oplogsCount=0;
		        	//oplogsCount.store(0);
	                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
   		         	smp_wmb();
                    node->writeUnlock();
                    return true;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            nodeKey = k[level];
            nextNodePtr = N::getChildPptr(nodeKey, node);
		    nextNode = nextNodePtr.getVaddr();

            if (nextNode == nullptr) {
                //if(node != locked_node){
                //if((node != locked_node) && (locked_node != nullptr)){
		//	printf("t:%lu 2 ART::INSERT node :%p\n",pthread_self(),node);
                    node->lockVersionOrRestart(v, needRestart,gId);
                    if (needRestart) goto restart;
	//	}
		        pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));

                N::insertAndUnlock(node, parentNode, parentKey, nodeKey, pTid, epocheInfo, needRestart, &oplogs[oplogsCount], genId);
		        oplogsCount=0;
	        	//oplogsCount.store(0);
                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
            	smp_wmb();
                if (needRestart) goto restart;
                return true;
            }
            if (N::isLeaf(nextNode)) {
         //       if(node != locked_node){
		//	printf("t:%lu 3 ART::INSERT node :%p\n",pthread_self(),node);
                	node->lockVersionOrRestart(v, needRestart, genId);
                    if (needRestart) goto restart;
	//	}
                Key key;
                loadKey(N::getLeaf(nextNode), key);
                if (key == k) {
                    // upsert
					 pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));
                    N::change(node, k[level], pTid);
		        	oplogsCount=0;
		        //	oplogsCount.store(0);
	                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
   		         	smp_wmb();
                    node->writeUnlock();
                    return false;
                }

                level++;
                assert(level < key.getKeyLen()); //prevent inserting when prefix of key exists already
                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }
#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif
/*        pptr<OpStruct> ologPtr;
		PMEMoid oid;
		PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
		OpStruct *olog = ologPtr.getVaddr();

 				pptr<N> n4Ptr;
                //PMem::alloc(poolId,sizeof(N4),(void **)&n4Ptr);
                PMem::alloc(poolId,sizeof(N4),(void **)&n4Ptr,&(olog->newNodeOid));*/
		pptr<N> n4Ptr;
                oplogs[oplogsCount].op = OpStruct::insert;
                oplogs[oplogsCount].oldNodePtr = (void*)nodePtr.getRawPtr();;
                PMem::alloc(poolId,sizeof(N4),(void **)&n4Ptr,&(oplogs[oplogsCount].newNodeOid));
		oplogsCount++;
                //PMem::++;
                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
            	smp_wmb();

				N4* n4= (N4*)new(n4Ptr.getVaddr()) N4(level+prefixLength,&k[level], prefixLength);
                pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));

                n4->insert(k[level + prefixLength], pTid,false);
                n4->insert(key[level + prefixLength], nextNodePtr,false);
				flushToNVM((char*)n4,sizeof(N4));
				smp_wmb();
                N::change(node, k[level - 1], n4Ptr);
                oplogs[oplogsCount].op = OpStruct::done;

				oplogsCount=0;	
//		        oplogsCount.store(0);
                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
            	smp_wmb();
                node->writeUnlock();

                return true;
            }
            level++;
        }
    }
#endif



#ifdef SYNC
	bool Tree::nodeUnlock(void* savenode, ThreadInfo &threadEpocheInfo) const {
		EpocheGuardReadonly epocheGuard(threadEpocheInfo);
		N *node =  reinterpret_cast<N *>(savenode);
//		printf("nodeUnlock::%p\n",node);
		node->writeUnlock();
		return true;
	}
#endif


    void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo) {
        EpocheGuard epocheGuard(epocheInfo);
        restart:
        bool needRestart = false;

        pptr<N> nextNodePtr = root;

        N *node = nullptr;
        N *nextNode = (N*)root.getVaddr();
        N *parentNode = nullptr;
	    pptr<N> nodePtr;
        pptr<N> parentPtr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            parentPtr = nodePtr;
            node = nextNode;
            nodePtr = nextNodePtr;	

            auto v = node->getVersion();

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix, this->loadKey)) { // increases level
                case CheckPrefixPessimisticResult::SkippedLevel:
                    goto restart;
                case CheckPrefixPessimisticResult::NoMatch: {
                    assert(nextLevel < k.getKeyLen()); //prevent duplicate key
                    node->lockVersionOrRestart(v, needRestart, genId);
                    if (needRestart) goto restart;

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    Prefix prefi = node->getPrefi();
                    prefi.prefixCount = nextLevel - level;
//                    auto newNode = new N4(nextLevel, prefi);

#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif


	           	    pptr<N> newNodePtr;
       /* pptr<OpStruct> ologPtr;
		PMEMoid oid;
		PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
		OpStruct *olog = ologPtr.getVaddr();
		    //PMem::alloc(poolId,sizeof(N4),(void **)&newNodePtr);
				    PMem::alloc(poolId,sizeof(N4),(void **)&newNodePtr, &(olog->newNodeOid));*/
                     oplogs[oplogsCount].op = OpStruct::insert;
                     oplogs[oplogsCount].oldNodePtr = (void *)parentPtr.getRawPtr();;
                    PMem::alloc(poolId,sizeof(N4),(void **)&newNodePtr, &(oplogs[oplogsCount].newNodeOid));
	   		        oplogsCount++;
                    flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
				    smp_wmb();

				    N4 *newNode= (N4*)new(newNodePtr.getVaddr()) N4(nextLevel,prefi);

		   			pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));

                    // 2)  add node and (tid, *k) as children
		    		    
                    newNode->insert(k[nextLevel], pTid,false);
                    newNode->insert(nonMatchingKey, nodePtr,false);
				    flushToNVM((char*)newNode,sizeof(N4));
				    smp_wmb();


                    // 3) lockVersionOrRestart, update parentNode to point to the new node, unlock
                    parentNode->writeLockOrRestart(needRestart, genId);
                    if (needRestart) {
                        //delete newNode;
						PMem::free((void *)newNodePtr.getRawPtr());
                        node->writeUnlock();
                        goto restart;
                    }
                    N::change(parentNode, parentKey, newNodePtr);
                     oplogs[oplogsCount].op = OpStruct::done;
                    parentNode->writeUnlock();

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix.prefix,
                                    node->getPrefi().prefixCount - ((nextLevel - level) + 1),true);


		        	oplogsCount=0;
		        	//oplogsCount.store(0);
	                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
   		         	smp_wmb();
                    node->writeUnlock();
                    return;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            nodeKey = k[level];
            nextNodePtr = N::getChildPptr(nodeKey, node);
		    nextNode = nextNodePtr.getVaddr();
            //nextNode = N::getChild(nodeKey, node, &nextNodePtr);
//	    printf("nextNode :%p\n",nextNode);

            if (nextNode == nullptr) {
                node->lockVersionOrRestart(v, needRestart, genId);
                if (needRestart) goto restart;

//		unsigned long leafPtr = tid.getPtr();
//		tid.setPtr((unsigned long)N::setLeaf(leafPtr));
		        pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));

                N::insertAndUnlock(node, parentNode, parentKey, nodeKey, pTid, epocheInfo, needRestart, &oplogs[oplogsCount], genId);
		        oplogsCount=0;
	        	//oplogsCount.store(0);
                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
            	smp_wmb();
                if (needRestart) goto restart;
                return;
            }
            if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart, genId);
                if (needRestart) goto restart;

                Key key;
	//	TID tid_test = N::getLeaf(nextNode);	
//                loadKey(tid_test,key);
                
				loadKey(N::getLeaf(nextNode), key);

		//TODO check loadKey
                if (key == k) {
                    // upsert
//		    unsigned long leafPtr = tid.getPtr();
//                    tid.setPtr((unsigned long)N::setLeaf(leafPtr));
	    	        pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));
                    N::change(node, k[level], pTid);
		        	oplogsCount=0;
		        //	oplogsCount.store(0);
	                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
   		         	smp_wmb();
                    node->writeUnlock();
                    return;
                }

                level++;
                assert(level < key.getKeyLen()); //prevent inserting when prefix of key exists already

                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                //auto n4 = new N4(level + prefixLength, &k[level], prefixLength);

#ifdef MULTIPOOL
    int chip,core;
    read_coreid_rdtscp(&chip,&core);
    uint16_t poolId = (uint16_t)(3*chip);
#else
    uint16_t poolId = 0;
#endif
                pptr<N> n4Ptr;
/*        pptr<OpStruct> ologPtr;
		PMEMoid oid;
		PMem::alloc(poolId,sizeof(OpStruct),(void **)&ologPtr, &oid);
		OpStruct *olog = ologPtr.getVaddr();
                //PMem::alloc(poolId,sizeof(N4),(void **)&n4Ptr);
                PMem::alloc(poolId,sizeof(N4),(void **)&n4Ptr,&(olog->newNodeOid));*/
                oplogs[oplogsCount].op = OpStruct::insert;
                oplogs[oplogsCount].oldNodePtr = (void*)nodePtr.getRawPtr();;
                PMem::alloc(poolId,sizeof(N4),(void **)&n4Ptr,&(oplogs[oplogsCount].newNodeOid));
		oplogsCount++;
                //PMem::++;
                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
            	smp_wmb();

				N4* n4= (N4*)new(n4Ptr.getVaddr()) N4(level+prefixLength,&k[level], prefixLength);
                pptr<N> pTid(0,(unsigned long)N::setLeaf(tid));

                n4->insert(k[level + prefixLength], pTid,false);
                n4->insert(key[level + prefixLength], nextNodePtr,false);
				flushToNVM((char*)n4,sizeof(N4));
				smp_wmb();
                N::change(node, k[level - 1], n4Ptr);
                oplogs[oplogsCount].op = OpStruct::done;

		oplogsCount=0;	
//		        oplogsCount.store(0);
                flushToNVM((char*)&oplogsCount,sizeof(uint64_t));
            	smp_wmb();
                node->writeUnlock();
                return;
            }
            level++;
        }
    }

    void Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
#if 0
        EpocheGuard epocheGuard(threadInfo);
        restart:
        bool needRestart = false;


	pptr<N> nullPtr(0,0);
        pptr<N> nextNodePtr = root;

        N *node = nullptr;
        N *nextNode = root.getVaddr();
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;
        //bool optimisticPrefixMatch = false;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->getVersion();

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
                        goto restart;
                    }
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    if (nextNode == nullptr) {
                        if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {//TODO benötigt??
                            goto restart;
                        }
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        node->lockVersionOrRestart(v, needRestart);
        		void *basePtr = mtable->getBasePtr(0);
                        if (needRestart) goto restart;
                        if (N::getLeaf(nextNode) != (unsigned long)(tid.getVaddr(basePtr))) {
                            node->writeUnlock();
                            return;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && node != (N*)(root.getVaddr(basePtr))) {
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                parentNode->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
			        idxPtr secondNodeNIdxPtr;	
				secondNodeNIdxPtr.setPtr((unsigned long)secondNodeN);
				
                                N::change(parentNode, parentKey, secondNodeNIdxPtr);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                uint64_t vChild = secondNodeN->getVersion();
                                secondNodeN->lockVersionOrRestart(vChild, needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    goto restart;
                                }
                                parentNode->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    secondNodeN->writeUnlock();
                                    goto restart;
                                }

			        idxPtr secondNodeNIdxPtr;	
				secondNodeNIdxPtr.setPtr((unsigned long)secondNodeN);
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeNIdxPtr);
                                secondNodeN->addPrefixBefore(node, secondNodeK);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                                secondNodeN->writeUnlock();
                            }
                        } else {
                            N::removeAndUnlock(node, k[level], parentNode, parentKey, threadInfo, needRestart, genId);
                            if (needRestart) goto restart;
                        }
                        return;
                    }
                    level++;
                }
            }
        }
#endif
    }


    typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (k.getKeyLen() <= n->getLevel()) {
            return CheckPrefixResult::NoMatch;
        }
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            level = n->getLevel();
            return CheckPrefixResult::OptimisticMatch;
        }
        if (p.prefixCount > 0) {
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
                 i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
                if (p.prefix[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (p.prefixCount > maxStoredPrefixLength) {
                level += p.prefixCount - maxStoredPrefixLength;
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level, uint8_t &nonMatchingKey, Prefix &nonMatchingPrefix, LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    loadKey(N::getAnyChildTid(n), kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : p.prefix[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (p.prefixCount > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            loadKey(N::getAnyChildTid(n), kt);
                        }
                        for (uint32_t j = 0; j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                                          maxStoredPrefixLength); ++j) {
                            nonMatchingPrefix.prefix[j] = kt[level + j + 1];
                        }
                    } else {
                        for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                            nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                        }
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint32_t &level, LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            return PCCompareResults::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            Key kt;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    loadKey(N::getAnyChildTid(n), kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : p.prefix[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end, LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            return PCEqualsResults::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            Key kt;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    loadKey(N::getAnyChildTid(n), kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : p.prefix[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }
    TID Tree::lookupNext(const Key &start, ThreadInfo &threadEpocheInfo) const {
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        TID toContinue = 0;
        bool restart;
        std::size_t resultsFound  = 0;
        std::size_t resultSize = 1;
        TID result[resultSize];
        std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy](const N *node) {
	    assert(node != nullptr);
            if (N::isLeaf(node)) {
		assert((void*)N::getLeaf(node) != nullptr);
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
		    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                N* child = N::getSmallestChild(node, 0);
		assert(child != nullptr);
                copy(child);
                /*
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != 0) {
                        break;
                    }
                }*/
            }
        };
        std::function<void(const N *)> copyReverse = [&result, &resultSize, &resultsFound, &toContinue, &copyReverse](const N *node) {
	    assert(node != nullptr);
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                N* child = N::getLargestChild(node, 255);
		assert(child != nullptr);
                copyReverse(child);
                /*
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (int i = childrenCount - 1; i >=  0; --i) {
                    const N *n = std::get<1>(children[i]);
                    copyReverse(n);
                    if (toContinue != 0) {
                        break;
                    }
                }*/
            }
        };
        std::function<void(N *, N*, uint32_t, uint32_t)> findStart = [&copy, &copyReverse, &start, &findStart, &toContinue, &restart,this](
                N *node, N *parentNode, uint32_t level, uint32_t parentLevel) {
	    assert(node != nullptr);
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }

            uint32_t initLevel = level;

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level, loadKey);
            switch (prefixResult) {
                case PCCompareResults::Bigger: {
                    N* childNode = nullptr;
                    if (start[parentLevel] != 0)
                        childNode = N::getLargestChild(parentNode, start[parentLevel] - 1);
                    if (childNode != nullptr)
                        copyReverse(childNode);
                    else
                        copy(node);
                    break;
                }
                case PCCompareResults::Smaller:
                    copyReverse(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    N *childNode = N::getChild(startLevel, node);
                    if(childNode != nullptr){
                        if(start[level] != 0) 
                            findStart(childNode, node, level + 1, level);
                        else
                            findStart(childNode, parentNode, level + 1, parentLevel);
                    }
                    else {
                        N* child = N::getLargestChild(node, startLevel);
                        if (child != nullptr){
                            copyReverse(child);
                        }
                        else {
                            N* childNode = nullptr;
                            if (start[parentLevel] != 0)
                                childNode = N::getLargestChild(parentNode, start[parentLevel] - 1);
                            if (childNode != nullptr){
                                copyReverse(childNode);
                            }
                            else {
                                child = N::getSmallestChild(node, startLevel);
                                copy(child);
                            }
                        }
                    }
                    break;
                }
                case PCCompareResults::SkippedLevel:
                    restart = true;
                    break;
            }
        };
        restart:
        restart = false;
        resultsFound = 0;

        uint32_t level = 0;
		pptr<N> nodePtr = root;
        N *node = nodePtr.getVaddr();
	assert(node != nullptr);
        findStart(node, node, level, level);
        if(restart)
            goto restart;
        return result[0];

    }
#ifdef SYNC
 TID Tree::lookupNextwithLock(const Key &start, void **savenode,  ThreadInfo &threadEpocheInfo) const {
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        TID toContinue = 0;
        bool restart;
        std::size_t resultsFound  = 0;
        std::size_t resultSize = 1;
        TID result[resultSize];
		uint64_t gId = genId;
        std::function<void(N *)> copy = [&result, &resultSize, &resultsFound, &toContinue,&restart, &gId, &savenode, &copy](N *node) {
        //std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue,&restart, &gId, &savenode, &copy](const N *node) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                N* child = N::getSmallestChild(node, 0);
				if(N::isLeaf(child)){
					if(node!=child){
						//					printf("case 1 p:%p\n",parent_node);
                    	node->writeLockOrRestart(restart, gId);
						//					printf("case 1-1 p:%p\n",parent_node);
						if (restart){
							return;
						}
						*savenode = (void*)node;
					}
				}
                copy(child);
                /*
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != 0) {
                        break;
                    }
                }*/
            }
        };
        std::function<void(N *)> copyReverse = [&result, &resultSize, &resultsFound, &toContinue, &restart,&gId,&savenode,&copyReverse](N *node) {
        //std::function<void(const N *)> copyReverse = [&result, &resultSize, &resultsFound, &toContinue, &restart,&gId,&savenode,&copyReverse](const N *node) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                N* child = N::getLargestChild(node, 255);
				if(N::isLeaf(child)){
					if(node!=child){
						//					printf("case 1 p:%p\n",parent_node);
                    	node->writeLockOrRestart(restart, gId);
						//					printf("case 1-1 p:%p\n",parent_node);
						if (restart){
							return;
						}
						*savenode = (void*)node;
					}
				}

				copyReverse(child);
                /*
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (int i = childrenCount - 1; i >=  0; --i) {
                    const N *n = std::get<1>(children[i]);
                    copyReverse(n);
                    if (toContinue != 0) {
                        break;
                    }
                }*/
            }
        };
        std::function<void(N *, N*, uint32_t, uint32_t)> findStart = [&copy, &copyReverse, &start, &findStart, &toContinue, &restart,this](
                N *node, N *parentNode, uint32_t level, uint32_t parentLevel) {
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }

            uint32_t initLevel = level;

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level, loadKey);
            switch (prefixResult) {
                case PCCompareResults::Bigger: {
                    N* childNode = nullptr;
                    if (start[parentLevel] != 0){
                        childNode = N::getLargestChild(parentNode, start[parentLevel] - 1);
					}
                    if (childNode != nullptr){
                        copyReverse(childNode);
					}
                    else{
                        copy(node);
					}
                    break;
                }
                case PCCompareResults::Smaller:
                    copyReverse(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    N *childNode = N::getChild(startLevel, node);
                    if(childNode != nullptr){
                        if(start[level] != 0) 
                            findStart(childNode, node, level + 1, level);
                        else
                            findStart(childNode, parentNode, level + 1, parentLevel);
                    }
                    else {
                        N* child = N::getLargestChild(node, startLevel);
                        if (child != nullptr){
                            copyReverse(child);
                        }
                        else {
                            N* childNode = nullptr;
                            if (start[parentLevel] != 0)
                                childNode = N::getLargestChild(parentNode, start[parentLevel] - 1);
                            if (childNode != nullptr){
                                copyReverse(childNode);
                            }
                            else {
                                child = N::getSmallestChild(node, startLevel);
                                copy(child);
                            }
                        }
                    }
                    break;
                }
                case PCCompareResults::SkippedLevel:
                    restart = true;
                    break;
            }
        };
        restart:
        restart = false;
        resultsFound = 0;

        uint32_t level = 0;
		pptr<N> nodePtr = root;
        N *node = nodePtr.getVaddr();
        findStart(node, node, level, level);
        if(restart)
            goto restart;
        return result[0];

    }
#endif
}
