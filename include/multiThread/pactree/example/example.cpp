// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include "pactree.h"
// #include "pactree_wrapper.h"
#include <numa-config.h>

#define NUMDATA 100

int main(int argc, char **argv){
    pactree *pt = new pactree(1);
    pt->registerThread();

    for(int i = 1; i < NUMDATA; i++) {
	pt->insert(i,i);
    }

    for(int i = 1; i < NUMDATA; i++) {
	if(i!=pt->lookup(i)){
		printf("error");
		exit(1);
	} else {
        printf("%d\n", i);
    }
    }
}
