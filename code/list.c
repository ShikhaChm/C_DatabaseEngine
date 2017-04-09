#include "list.h"

linkedList *listCreate()
{
    return calloc(1, sizeof(linkedList));
}

void listDestroy(linkedList *list)
{
    linkedListNode *cur = NULL;
    for(cur = list->head; cur != NULL; cur=cur->next) {
        if(cur->prev) {
            free(cur->prev);
        }
    }

    free(list->tail);
    free(list);
}


void listClear(linkedList *list)
{
    linkedListNode *cur = NULL;
    for(cur = list->head; cur != NULL; cur=cur->next) {
        free(cur->value);
    }

}


void listClearDestroy(linkedList *list)
{
    listClear(list);
    listDestroy(list);
}


void listAddToEnd(linkedList *list, void *value)
{
    linkedListNode *node = calloc(1, sizeof(linkedListNode));
    node->value = value;

    if(list->tail == NULL) {
        list->head = node;
        list->tail = node;
    } else {
        list->tail->next = node;
        node->prev = list->tail;
        list->tail = node;
    }

    list->count++;

    return;
}

void *listDequeue(linkedList *list)
{
    linkedListNode *node = list->tail;
    return node != NULL ? listRemove(list, node) : NULL;
}

void listPush(linkedList *list, void *value)
{
    linkedListNode *node = calloc(1, sizeof(linkedListNode));

    node->value = value;

    if(list->head == NULL) {
        list->head = node;
        list->tail = node;
    } else {
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }

    list->count++;

    return;
}

void *listPop(linkedList *list)
{
    linkedListNode *node = list->head;
    return node != NULL ? listRemove(list, node) : NULL;
}


void *listRemove(linkedList *list, linkedListNode *node)
{
    void *result = NULL;

    if(!(list->head && list->tail)) {
	printf("list is empty\n");
	return;
    }
    if(!(node)) {
	printf("Node Cant be Null\n");
	return;
    }

    if(node == list->head && node == list->tail) {
        list->head = NULL;
        list->tail = NULL;
    } else if(node == list->head) {
        list->head = node->next;
	if(list->head == NULL) {
		printf("Invalid list, somehow got a head that is NULL.\n");
		return;
	}
        list->head->prev = NULL;
    } else if (node == list->tail) {
        list->tail = node->prev;
	if(list->tail == NULL) {
		printf("Invalid list, somehow got a next that is NULL.\n");
		return;
	}
        list->tail->next = NULL;
    } else {
        linkedListNode *after = node->next;
        linkedListNode *before = node->prev;
        after->prev = before;
        before->next = after;
    }

    list->count--;
    result = node->value;
    free(node);

    return result;
}
