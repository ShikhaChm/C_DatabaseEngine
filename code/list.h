#ifndef linkedList_h
#define linkedList_h

#include <stdlib.h>
#include <stdio.h>


struct linkedListNode;

/**
 * This is a node for a doubly linked list linkedList. it contains its data as (void*).
 */
typedef struct linkedListNode {
  /*@{*/
    /** The next pointer of the linked list*/
    struct linkedListNode *next; 
    /** The previous pointer of the linked list*/
    struct linkedListNode *prev; 
    /** The data cotained in this node. When used in linkedList for BM_BufferPool, BM_MetaPage type of data is stored in this field */
    void *value; 
  /*@}*/
} linkedListNode;


/**
 * This is a doubly linked list for storing pages in the buffer pool. Doubly linked list allows me the flexibility to add and remove at both ends. This allows me to do basic operations like fifo additions in constant time (independent of the length of the linked list).
 */
typedef struct linkedList {
  /*@{*/
    int count;
    /** The head pointer of the doubly linked list*/
    linkedListNode *head; 
    /** The tail pointer of the doubly linked list*/
    linkedListNode *tail; 
  /*@}*/
} linkedList;


/**
 * Creates and empty list with both head and tail pointing to NULL.
 */
linkedList *listCreate();

/**
 * Iterates on each node and deallocates the node and then de allocates the list.
 * @param[in] list The list that we want to deallocate and destroy.
 */
void listDestroy(linkedList *list);

/**
 * Iterates on each node of the list and deallocates the data (void *value) in the node.
 * @param[in] list The list that we want to clear
 */
void listClear(linkedList *list);

/**
 * First it clears the list using listClear, and then it destroys the list using listDestroy
 * @param[in] list The list that we want to clear and destroy 
 */
void listClearDestroy(linkedList *list);



/**
 * Adds a new node containing data *value at the tail of the list.
 * @param[in] list The list to which we want to add
 * @param[in] value The value that we want to put in the new node (added at tail).
 */
void listAddToEnd(linkedList *list, void *value);

/**
 * Removes a node from the tail end of the list.
 * @param[in] list The list that we want to dequeue (at tail)
 */
void *listDequeue(linkedList *list);


/**
 * Adds a new node containing data *value at the head of the list.
 * @param[in] list The list to which we want to add
 * @param[in] value The value that we want to put in the new node (added at head).
 */
void listPush(linkedList *list, void *value);

/**
 * Removes a node from the head end of the list.
 * @param[in] list The list that we want to pop (from head)
 */
void *listPop(linkedList *list);



/**
 * Removes a node from list , if the node is present in the list.
 * @param[in] list The list from which we want to delete 
 * @param[in] node The node that we want to remove from *list 
 */
void *listRemove(linkedList *list, linkedListNode *node);

#endif

