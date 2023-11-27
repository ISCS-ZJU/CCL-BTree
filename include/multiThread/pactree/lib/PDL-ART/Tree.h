//
// Created by florian on 18.11.15.
//

#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H
#include "N.h"
#include "Key.h"
#include "Epoche.h"


using namespace ART;

namespace ART_ROWEX {

    class Tree {
    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);
	uint64_t genId;

    private:
        pptr<N> root;
        //N *const root;

   TID checkKey(const TID tid, const Key &k) const;

    LoadKeyFunction loadKey;

    Epoche epoche{256};
	OpStruct oplogs[10000];
	//OpStruct oplogs[10000000];

//	OpStruct oplogs[100000];
//	std::atomic<uint64_t> oplogsCount{0}; 

	uint64_t oplogsCount;

//	static mappingTable mtable;
    public:
/*	void *operator new(size_t size){
		return allocNVM(size);
	}

	void operator delete(void *ptr){
		return freeNVM(ptr);
	}*/



        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
            SkippedLevel
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
            SkippedLevel
        };
        enum class PCEqualsResults : uint8_t {
            PartialMatch,
            BothMatch,
            Contained,
            NoMatch,
            SkippedLevel
        };
        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey);

        static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint32_t &level, LoadKeyFunction loadKey);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, LoadKeyFunction loadKey);

    public:

        void recover();
        Tree(LoadKeyFunction loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        ThreadInfo getThreadInfo() ;

        TID lookup(const Key &k, ThreadInfo &threadEpocheInfo) const;

        TID lookupNext(const Key &k, ThreadInfo &threadEpocheInfo) const;

        void insert(const Key &k, TID tid, ThreadInfo &epocheInfo);
        void remove(const Key &k, TID tid, ThreadInfo &epocheInfo);

#ifdef SYNC
		TID lookupNextwithLock(const Key &start, void** savenode, ThreadInfo &threadEpocheInfo) const;
		bool nodeUnlock(void* savenode, ThreadInfo &threadEpocheInfo) const;
		bool insert(const Key &k, TID tid, ThreadInfo &epocheInfo, void *locked_node);
#endif



        bool isEmpty();
    };

}
#endif //ART_ROWEX_TREE_H
