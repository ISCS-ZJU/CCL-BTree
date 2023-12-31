#!/bin/bash

num_keys=100000000

# CWARMING="-Wall" 
CWARMING="-w" 
# CDEBUG="-DNDEBUG"
CXXFLAG="-lpmem -lpmemobj -lpthread -ltbb -march=native"
CPPPATH="include/tools/mempool_numa.c include/tools/log_numa.c" 
threads=(47)
scansize=(100)

if [ $# -ne 1 ]
then
    defines=""
    for para in $@
    do
        if [ $para = "eADR" ]; then
        threads=(1 12 24 36 47 60 72 84 95)
        defines=$defines" -DeADR_TEST"
        fi
        
        if [ $para = "dramspace" ]; then
        defines=$defines" -DDRAM_SPACE_TEST" 
        fi
       
        if [ $para = "nobuffer" ]; then
        defines=$defines" -DTREE_NO_BUFFER"
        fi
        
        if [ $para = "noseleclog" ]; then
        defines=$defines" -DTREE_NO_SELECLOG"
        fi
        
        if [ $para = "nosearchcache" ]; then
        defines=$defines" -DTREE_NO_SEARCHCACHE" 
        fi
       
        if [ $para = "scan" ]; then
        scansize=(20 50 200 400)
        defines=$defines" -DDO_INSERT -DDO_SCAN" 
        fi

    done
else
    threads=(24 72)
    defines="-DDO_UPDATE -DDO_SEARCH"
fi

defines=$defines" -DNUMA_TEST"

definesdp=$defines
defines=$defines" -DUNIFIED_NODE"

echo $defines

if [ $1 = "cclbtree_ff" ] || [ $1 = "all" ]; then
echo "*******************normal test: cclbtree_ff*************************"
g++ -I include/multiThread/cclbtree_ff -I include $defines -DCCLBTREE_FF -O3 $CDEBUG $CWARMING -o mybin/m_normal_test_cclbtree_ff test/multiThread/normal_test.cpp $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_cclbtree_ff $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi


if [ $1 = "cclbtree_lb" ] || [ $1 = "all" ]; then
echo "*******************normal test: cclbtree_lb*************************"
g++ -I include/multiThread/cclbtree_lb -I include $defines -DCCLBTREE_LB -O3 $CDEBUG $CWARMING -o mybin/m_normal_test_cclbtree_lb test/multiThread/normal_test.cpp $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_cclbtree_lb $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi


if [ $1 = "lbtree" ] || [ $1 = "all" ]; then
echo "*******************normal test: lbtree*************************"
g++ -I include/multiThread/lbtree/lbtree-src -I include/multiThread/lbtree/common -I include $defines -DLBTREE -O3 $CDEBUG $CWARMING -o mybin/m_normal_test_lbtree test/multiThread/normal_test.cpp include/multiThread/lbtree/common/tree.cc include/multiThread/lbtree/lbtree-src/lbtree.cc $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_lbtree $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi



DPTREESRC="include/multiThread/dptree/src/"
CPPPATH_DPTREE="${DPTREESRC}art_idx.cpp ${DPTREESRC}ART.cpp ${DPTREESRC}bloom.c ${DPTREESRC}Epoche.cpp ${DPTREESRC}MurmurHash2.cpp  ${DPTREESRC}Tree.cpp ${DPTREESRC}dptree_util.cpp"

if [ $1 = "dptree" ] || [ $1 = "all" ]; then
echo "*******************normal test: dptree*************************"
g++ -I include/multiThread/dptree/include -I include $definesdp -DDPTREE -O3 $CDEBUG $CWARMING -o mybin/m_normal_test_dptree test/multiThread/normal_test.cpp $CPPPATH_DPTREE $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_dptree $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi



if [ $1 = "utree" ] || [ $1 = "all" ]; then
echo "*******************normal test: utree*************************"
g++ -I include/multiThread/utree -I include $defines -DUTREE -O3 $CDEBUG  $CWARMING -o mybin/m_normal_test_utree test/multiThread/normal_test.cpp $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_utree $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi



if [ $1 = "fastfair" ] || [ $1 = "all" ]; then
echo "*******************normal test: fastfair*************************"
g++ -I include/multiThread/fast_fair -I include $defines -DFASTFAIR -O3 $CDEBUG  $CWARMING -o mybin/m_normal_test_fastfair test/multiThread/normal_test.cpp $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_fastfair $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi

if [ $1 = "fptree" ] || [ $1 = "all" ]; then
echo "*******************normal test: fptree*************************"
g++ -I include/multiThread/fptree -I include $defines -DFPTREE -O3 $CDEBUG  $CWARMING -o mybin/m_normal_test_fptree test/multiThread/normal_test.cpp include/multiThread/fptree/fptree.cpp $CPPPATH $CXXFLAG
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_fptree $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi

PACTREESRC="include/multiThread/pactree/src/"
CPPPATH_PACTREE="${PACTREESRC}linkedList.cpp ${PACTREESRC}listNode.cpp  ${PACTREESRC}Oplog.cpp  ${PACTREESRC}pactree.cpp  ${PACTREESRC}WorkerThread.cpp ${PACTREESRC}pactree_wrapper.cpp
"

CPPLINK="-lpthread -ltbb"
if [ $1 = "pactree" ] || [ $1 = "all" ]; then
echo "*******************normal test: pactree*************************"
g++ -I include/multiThread/pactree/include -I include/multiThread/pactree/src -I include $defines -DPACTREE -O3 $CDEBUG $CWARMING -o mybin/m_normal_test_pactree test/multiThread/normal_test.cpp $CPPPATH_PACTREE $CPPPATH $CXXFLAG $CPPLINK -L ${PACTREESRC} -lpdlart
wait
for num_threads in ${threads[@]} 
do
for ssize in ${scansize[@]}
do
numactl --membind=0,1 --cpunodebind=0,1 ./mybin/m_normal_test_pactree $num_keys $num_threads $ssize
wait
rm -rf /mnt/pmem/cclbtree/*
rm -rf /pmem/cclbtree/*
wait
done
done
fi

# pactree test memory footprint:  -DMEMORY_FOOTPRINT