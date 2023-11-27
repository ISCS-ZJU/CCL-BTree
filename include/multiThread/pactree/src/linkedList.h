// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef _LINKEDLIST_H
#define _LINKEDLIST_H
#include "listNode.h"
//#define DIST

enum Operation  {lt, gt};

class LinkedList {
private:
    pptr<ListNode> headPtr;
    OpStruct oplogs[100000]; // 500 *200
    int nunOpLogs[112];
    uint64_t genId;

public:
    ListNode* initialize();
#ifdef SYNC
    bool insert(Key_t key, Val_t value, ListNode* head,void** locked_parent_node);
#endif
    bool insert(Key_t key, Val_t value, ListNode* head, int threadId);
    bool update(Key_t key, Val_t value, ListNode* head);
    bool remove(Key_t key, ListNode* head);
    bool probe(Key_t key, ListNode* head);
    bool lookup(Key_t key, Val_t &value, ListNode* head);
    uint64_t scan(Key_t startKey, int range, std::vector<Val_t> &rangeVector, ListNode *head);
    void print(ListNode *head);
    uint32_t size(ListNode* head);
    ListNode* getHead();
    bool Recovery(void* sl);
};
void printDists();

#endif //_LINKEDLIST_H
