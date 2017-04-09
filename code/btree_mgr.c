#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "btree_mgr.h"

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData)
{
	return RC_OK;
}

extern RC shutdownIndexManager ()
{
	return RC_OK;
}


// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n)
{
	int i;
	char * idxInfo, *rootNodePage;
	VarString *result;
	Value *dummy = (Value *) malloc(sizeof(Value));
	MAKE_VARSTRING(result);
	//Create Meta Page
	APPEND(result, "NumEntries:%i\n", 0);
	APPEND(result, "NumNodes:%i\n", 1);
	APPEND(result, "FanOut:%i\n", n);
	dummy->dt = keyType;
	switch (keyType)
	{
	    case DT_INT:
		APPEND(result, "KeyType:%s\n","INT");
		dummy->v.intV = minINF;
		break;
	    case DT_FLOAT:
		APPEND(result, "KeyType:%s\n","FLOAT");
		dummy->v.floatV = (float)minINF;
		break;
	    case DT_STRING:
		APPEND(result, "KeyType:%s\n","STRING");
		dummy->v.stringV = " ";
		break;
	    case DT_BOOL:
		APPEND(result, "KeyType:%s\n","BOOL");
		dummy->v.boolV = FALSE;
		break;
	}
	APPEND(result, "RootNode:%i\n",1);
	APPEND(result, "MaxPage:%i\n", 1);
  	GET_STRING(idxInfo,result);  
	FREE_VARSTRING(result);

	//Create rootNode empty Page
	MAKE_VARSTRING(result);
	APPEND(result, "NodeNum:%i\n", 1);
	APPEND(result, "Parent:%i\n", -1);
	APPEND(result, "NumKeys:%i\n", 0);
	APPEND(result, "IsLeaf:%i\n", 1);
	APPEND(result, "Keys:[");
	for(i = 0; i < n ; i++) {
		APPEND(result, "%s%s",serializeValue(dummy),(i==n-1) ? "": ",");
	}
	APPEND(result, "]\n");
	APPEND(result, "Child:[");
	for(i = 0; i < n+1 ; i++) {
		APPEND(result, "%s%s","-1.-1",(i==n) ? "": ",");
	}
	APPEND(result, "]\n");
	APPEND(result, "Next:%i\n",-1);
  	GET_STRING(rootNodePage,result);  
	FREE_VARSTRING(result);
	//Write
	createPageFile(idxId);
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	initBufferPool(bm,idxId, IDX_BUFFER_SIZE, RS_FIFO, NULL);
	pinPage(bm,h,0);
	sprintf(h->data, "%s", idxInfo);
	markDirty(bm,h);
	unpinPage(bm,h);

	pinPage(bm,h,1);
	sprintf(h->data, "%s", rootNodePage);
	markDirty(bm,h);
	unpinPage(bm,h);
	
	free(h);
	free(rootNodePage);	
	shutdownBufferPool(bm);
	free(bm);
	freeValue(dummy);
	return RC_OK;
}

extern RC openBtree (BTreeHandle **tree, char *idxId)
{
	int nEntries, nNodes,fOut,rootPage, maxPage;
	DataType kType;
	char *page = (char*) malloc(PAGE_SIZE);
	BM_PageHandle *h;
	BTreeMeta *btmData = (BTreeMeta *)malloc(sizeof(BTreeMeta));

	btmData->bm = MAKE_POOL();
	initBufferPool(btmData->bm,idxId,IDX_BUFFER_SIZE,RS_FIFO,NULL);
	h = MAKE_PAGE_HANDLE();
	pinPage(btmData->bm,h,0);
	strncpy(page,h->data,PAGE_SIZE);
	unpinPage(btmData->bm,h);
	stringToIdxInfo(page,&nEntries,&nNodes,&fOut,&kType,&rootPage,&maxPage);
	btmData->numEntries = nEntries;
	btmData->numNodes = nNodes;
	btmData->fanOut = fOut;
	btmData->maxPageNum = maxPage;

	btmData->root = (BTNode *) malloc(sizeof(BTNode));
	pinPage(btmData->bm,h,rootPage);
	strncpy(page,h->data,PAGE_SIZE);
	unpinPage(btmData->bm,h);
	btmData->root = stringToNode(page);
	(*tree) = (BTreeHandle *) malloc(sizeof(BTreeHandle));
	(*tree)->keyType = kType;
	(*tree)->idxId = idxId;
	(*tree)->mgmtData = btmData;
	free(h);
	return RC_OK;
}

extern RC closeBtree (BTreeHandle *tree)
{
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData;
	
	if(shutdownBufferPool(btmData->bm) == RC_BM_POOL_HAS_PINNED_PAGES) {
		return RC_IM_CANNOT_CLOSE_IDX_IN_USE ;
	}	
	free(btmData->bm);	
	freeNode(&btmData->root, tree);	
	free(btmData);
	//free(tree->idxId);
	free(tree);
	return RC_OK;
}

extern RC deleteBtree (char *idxId)
{
	destroyPageFile(idxId);
	return RC_OK;
}


// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result)
{
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData ;
	(*result) = btmData->numNodes;
	return RC_OK;
}

extern RC getNumEntries (BTreeHandle *tree, int *result)
{
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData ;
	(*result) = btmData->numEntries;
	return RC_OK;
}

extern RC getKeyType (BTreeHandle *tree, DataType *result)
{
	(*result) = tree->keyType;
	return RC_OK;
}
extern int getRootPageNum(BTreeHandle *tree) {
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData ;
	return btmData->root->nodeNum;
}
extern int getFanOut(BTreeHandle *tree) {
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData ;
	return btmData->fanOut;
}


// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
	return recursiveFindKey(tree, key, result, getRootPageNum(tree));
}
extern RC recursiveFindKey (BTreeHandle *tree, Value *key, RID *result,int nodePage)
{
	//Read Node
	int pos,i,leftSize,rightSize;
	Value *tmpVal;
	BTNode *node, *left, *right, *newRoot;
	char *page = (char*) malloc(PAGE_SIZE);
	node = readNode(nodePage,tree);
	if(!node->isLeaf) {
		//If not leaf recurse
		pos=0;
		for(i=0; i < node->numKeys; i++) {
			tmpVal = node->keys[i];
			if(key->v.intV < tmpVal->v.intV) break;
			else pos=pos+1;
		}
		return recursiveFindKey(tree, key, result, node->childPage[pos]);
	} else {
		//If leaf :
		pos=0;
		for(i=0; i < node->numKeys; i++) {
			tmpVal = node->keys[i];
			if(key->v.intV == tmpVal->v.intV) {
				(*result) = node->childRid[i];
				pos=1;
				break;
			}
		}
		if(pos==0) return RC_IM_KEY_NOT_FOUND;
	
	}
	return RC_OK;	
}

extern RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	RID tmp;
	//Find key. If key not found then insert. else error.
	if(findKey(tree,key, &tmp) == RC_IM_KEY_NOT_FOUND) {
		recursiveInsertKey(tree, key, rid, getRootPageNum(tree));
	} else return RC_IM_KEY_ALREADY_EXISTS;
	return RC_OK;
}
extern RC recursiveInsertKey (BTreeHandle *tree, Value *key, RID rid, int nodePage) 
{
	//Read Node
	int pos,i,leftSize,rightSize,fanOut;
	Value *tmpVal;
	BTNode *node, *left, *right, *newRoot;
	BTreeMeta *btmData = (BTreeMeta *)tree->mgmtData;
	node = readNode(nodePage,tree);
	fanOut = getFanOut(tree);
	if(!node->isLeaf) {
		//If not leaf recurse
		pos=0;
		for(i=0; i < node->numKeys; i++) {
			tmpVal = node->keys[i];
			if(key->v.intV < tmpVal->v.intV) break;
			else pos=pos+1;
		}
		return recursiveInsertKey(tree, key, rid, node->childPage[pos]);
	} else {
		//If leaf :check empty slot 
		if(node->numKeys < fanOut) {
			//If leaf with empty slot : insert
			pos=0;
			for(i=0; i < node->numKeys; i++) {
				tmpVal = node->keys[i];
				if(key->v.intV < tmpVal->v.intV) break;
				else pos=pos+1;
			}
			if(pos < node->numKeys) {
				for(i=node->numKeys; i>pos ;i--) {
					copyValue( node->keys[i-1],node->keys[i]);
					node->childRid[i].page = node->childRid[i-1].page;
					node->childRid[i].slot= node->childRid[i-1].slot;
				} 
			}
			copyValue(key,node->keys[pos]);
			node->childRid[pos].page = rid.page;
			node->childRid[pos].slot= rid.slot;
			node->numKeys = node->numKeys + 1;
			btmData->numEntries = btmData->numEntries + 1;
			//commit to disk : node and meta
			writeTreeInfo(tree);
			writeNode(node, tree);	
		} else {
			//If leaf without empty slot : split , (if not root then backpropagate);
			pos=0;
			for(i=0; i < node->numKeys; i++) {
				tmpVal = node->keys[i];
				if(key->v.intV < tmpVal->v.intV) break;
				else pos=pos+1;
			}
	
			left = node;
			createNewNodeLeaf(&right,fanOut);
			right->nodeNum= btmData->maxPageNum + 1;
			left->isLeaf = 1;
			right->isLeaf = 1;
			right->next = left->next;
			left->next = right->nodeNum;
			//if even fanout;
			leftSize =1+(fanOut)/2;
			rightSize = (fanOut)/2;
			if(fanOut % 2) {
				//if odd fanout
				leftSize  = (fanOut+1)/2;
				rightSize = (fanOut+1)/2;
			}
			if(pos < leftSize) {
				//newKey left
				for(i=(leftSize-1); i < left->numKeys ; i++) {
					copyValue(left->keys[i],right->keys[i - leftSize + 1]);
					right->childRid[i -leftSize + 1].page = left->childRid[i].page;
					right->childRid[i -leftSize + 1].slot= left->childRid[i].slot;
					left->keys[i]->v.intV = minINF;
					left->childRid[i].page = -1;
					left->childRid[i].slot = -1;
				}
				for(i=leftSize-1; i>pos ;i--) {
					copyValue( left->keys[i-1],left->keys[i]);
					left->childRid[i].page = left->childRid[i-1].page;
					left->childRid[i].slot= left->childRid[i-1].slot;
				} 
				left->keys[pos]->v.intV = key->v.intV;
				left->childRid[pos].page = rid.page;
				left->childRid[pos].slot= rid.slot;
			} else {
				//newKey right
				for(i=leftSize; i < left->numKeys ; i++) {
					copyValue(left->keys[i], right->keys[ i - leftSize]) ;
					right->childRid[i - leftSize].page = left->childRid[i].page;
					right->childRid[i - leftSize].slot = left->childRid[i].slot;
					left->keys[i]->v.intV = minINF;
					left->childRid[i].page = -1;
					left->childRid[i].slot = -1;
				}
				for(i=rightSize-1; i>(pos-leftSize) ;i--) {
					copyValue(right->keys[i-1],right->keys[i]);
					right->childRid[i].page = right->childRid[i-1].page;
					right->childRid[i].slot = right->childRid[i-1].slot;
				} 
				right->keys[pos-leftSize]->v.intV = key->v.intV;
				right->childRid[pos-leftSize].page = rid.page;
				right->childRid[pos-leftSize].slot= rid.slot;
			}
			left->numKeys = leftSize;
			right->numKeys = rightSize;

			if(left->nodeNum == getRootPageNum(tree)) {
				//leaf node is root create new root
				createNewNodeInternal(&newRoot,fanOut);
				newRoot->nodeNum= btmData->maxPageNum + 2;

				btmData->numEntries = btmData->numEntries + 1;
				btmData->numNodes = btmData->numNodes + 2;
				btmData->maxPageNum =  btmData->maxPageNum + 2;
				btmData->root = newRoot;

				left->parent = newRoot->nodeNum;
				right->parent = newRoot->nodeNum;
				newRoot->parent = -1;

				newRoot->isLeaf = 0;

				newRoot->next = -1;
				newRoot->numKeys = 1;
				copyValue(right->keys[0], newRoot->keys[0] );
				newRoot->childPage[0] = left->nodeNum;
				newRoot->childPage[1] = right->nodeNum;
				//Write meta , root
				writeTreeInfo(tree);
				writeNode(newRoot,tree);
			} else {
				right->parent= left->parent;
				//leaf node isn't root, backPropagate
				btmData->numEntries = btmData->numEntries + 1;
				btmData->numNodes = btmData->numNodes + 1;
				btmData->maxPageNum =  btmData->maxPageNum + 1;
				//write meta
				writeTreeInfo(tree);
				insertBackPropagate(tree,right->keys[0],right->nodeNum,left->parent);
			}
			//write left, right
			writeNode(left,tree);
			writeNode(right,tree);
			//free left right
			freeNode(&left,tree);
			freeNode(&right,tree);

		}
	}
	//Free
	return RC_OK;	
}
extern RC insertBackPropagate(BTreeHandle *tree, Value *key, int newPage, int parentPage) 
{
	//if parentPage has empty slot , insert newPage
	//if parentPage is filled : split
	//if parentPage is filled, and is root :  create new root
	//if parentPage is filled, and is not root : backPropagate
	int pos,i,leftSize,rightSize,fanOut;
	Value *tmpVal, *upKey;
	BTNode *node, *left, *right, *newRoot;
	BTreeMeta *btmData = (BTreeMeta *)tree->mgmtData;
	node = readNode(parentPage,tree);
	fanOut = getFanOut(tree);
	//If empty slot : insert
	pos=0;
	for(i=0; i < node->numKeys; i++) {
		tmpVal = node->keys[i];
		if(key->v.intV < tmpVal->v.intV) break;
		else pos=pos+1;
	}
	if(pos < node->numKeys) {
		for(i=node->numKeys; i>pos ;i--) {
			copyValue( node->keys[i-1],node->keys[i]);
			node->childPage[i+1] = node->childPage[i];
		}
	}
	copyValue(key,node->keys[pos]);
	node->childPage[pos+1] = newPage;
	if(node->numKeys < fanOut) {
		node->numKeys = node->numKeys + 1;
		//commit to disk		
		writeNode(node,tree);
	} else {
		//If without empty slot : split ;
		//if even fanout;
		upKey = (Value *) malloc(sizeof(Value));
		pos= node->numKeys/2;
		leftSize  = (fanOut)/2;
		rightSize = (fanOut)/2;
		if(fanOut % 2) {
			//if odd fanout
			pos=(1+ node->numKeys)/2;
			leftSize  = (fanOut+1)/2;
			rightSize = (fanOut+1)/2 - 1;
		}
		copyValue(node->keys[pos],upKey);	
		left = node;
		createNewNodeInternal(&right,fanOut);
		right->nodeNum= btmData->maxPageNum + 1;
		right->next = left->next;
		left->next = right->nodeNum;
		for(i=pos+1; i < left->numKeys+1 ; i++) {
			copyValue(left->keys[i],right->keys[i - (pos+1)]);
			left->keys[i]->v.intV = minINF;
		}
		left->keys[pos]->v.intV = minINF;
		for(i=pos+1; i < left->numKeys+2 ; i++) {
			right->childPage[i -(pos+1)] = left->childPage[i];
			left->childPage[i] = -1;
		}
		left->numKeys = leftSize;
		right->numKeys = rightSize;
		if(left->nodeNum == getRootPageNum(tree)) {
			//if initial parent  node is root create new root
			createNewNodeInternal(&newRoot,fanOut);
			newRoot->nodeNum= btmData->maxPageNum + 2;

			//btmData->numEntries = btmData->numEntries + 1;
			btmData->numNodes = btmData->numNodes + 2;
			btmData->maxPageNum =  btmData->maxPageNum + 2;
			btmData->root = newRoot;

			left->parent = newRoot->nodeNum;
			right->parent = newRoot->nodeNum;
			newRoot->parent = -1;

			newRoot->isLeaf = 0;

			newRoot->next = -1;
			newRoot->numKeys = 1;

			copyValue(upKey, newRoot->keys[0] );
			newRoot->childPage[0] = left->nodeNum;
			newRoot->childPage[1] = right->nodeNum;
			//Write meta , root 
			writeTreeInfo(tree);
			writeNode(newRoot,tree);
		} else {
			right->parent= left->parent;
			//leaf node isn't root, backPropagate
			btmData->numNodes = btmData->numNodes + 1;
			btmData->maxPageNum =  btmData->maxPageNum + 1;
			writeTreeInfo(tree);
			insertBackPropagate(tree,upKey,right->nodeNum,left->parent);
		}
		//write left right
		writeNode(left,tree);
		writeNode(right, tree);
		freeNode(&left, tree);
		freeNode(&right, tree);
		free(upKey);
	}
		
	//Free
	return RC_OK;
}

extern RC deleteKey (BTreeHandle *tree, Value *key)
{
	RID tmp;
	//Find key. If key found then delete. else error.
	if(findKey(tree,key, &tmp) == RC_OK) {
		recursiveDeleteKey(tree, key, getRootPageNum(tree));
	} else return RC_IM_KEY_NOT_FOUND;
	return RC_OK;
}

extern RC recursiveDeleteKey (BTreeHandle *tree, Value *key, int nodePage) 
{
	//Read Node
	int pos,i, ufThresh,fanOut;
	bool redist;
	Value *tmpVal;
	BTNode *node, *left, *right, *newRoot, *parent;
	BTreeMeta *btmData = (BTreeMeta *)tree->mgmtData;
	fanOut = getFanOut(tree);
	node = readNode(nodePage,tree);
	if(!node->isLeaf) {
		//If not leaf recurse
		pos=0;
		for(i=0; i < node->numKeys; i++) {
			tmpVal = node->keys[i];
			if(key->v.intV < tmpVal->v.intV) break;
			else pos=pos+1;
		}
		return recursiveDeleteKey(tree, key, node->childPage[pos]);
	} else {
		//If leaf :
		pos=0;
		for(i=0; i < node->numKeys; i++) {
			tmpVal = node->keys[i];
			if(key->v.intV == tmpVal->v.intV) {
				pos=i;
				break;
			}
		}
		//Delete Key and Child
		for(i=pos; i < node->numKeys ; i++) {
			copyValue(node->keys[i+1],node->keys[i]);
			node->childRid[i].page = node->childRid[i+1].page;
			node->childRid[i].slot= node->childRid[i+1].slot;
		}
		node->numKeys = node->numKeys - 1;
		//If leaf is not in underflow, or leaf is root : Done
		ufThresh = fanOut/2;
		if(fanOut%2) {
			ufThresh = (fanOut+1)/2;
		} 
		if((getRootPageNum(tree) == node->nodeNum) || (node->numKeys >= ufThresh)) {
			writeNode(node,tree);
			return RC_OK;
		}
		//If non root leaf is in underflow, check redistribution
		parent = readNode(node->parent,tree);
		pos=0;
		for(i=0; i < parent->numKeys+1; i++) {
			if(parent->childPage[i] == node->nodeNum) {
				pos=i;
				break;
			}
		}
		redist = false;
		if(pos < parent->numKeys) {
			//attempt redistribution on right sibling
			right = readNode(parent->childPage[pos+1],tree);
			if(right->numKeys > ufThresh) {
				for(i=0 ; i < (ufThresh - node->numKeys) ; i++) {
					//copy right to node
					copyValue(right->keys[i], node->keys[node->numKeys+i]);
					node->childRid[node->numKeys+i].page = right->childRid[i].page;
					node->childRid[node->numKeys+i].slot= right->childRid[i].slot;
				} 
				for(i=(ufThresh-node->numKeys) ; i < right->numKeys ; i++) {
					//shift node in right sibling
					copyValue(right->keys[i], right->keys[i-(ufThresh - node->numKeys)]);
					right->childRid[i-(ufThresh - node->numKeys)].page = right->childRid[i].page;
					right->childRid[i-(ufThresh - node->numKeys)].slot = right->childRid[i].slot;
				}
				for(i=0; i < (ufThresh-node->numKeys) ; i++) {
					right->keys[right->numKeys-(ufThresh-node->numKeys)+i]->v.intV=minINF;
					right->childRid[right->numKeys-(ufThresh-node->numKeys)+i].page=-1;
					right->childRid[right->numKeys-(ufThresh-node->numKeys)+i].slot=-1;
				}
				//update parent key
				copyValue(right->keys[0],parent->keys[pos]);
				right->numKeys = right->numKeys - (ufThresh - node->numKeys);
				node->numKeys = ufThresh;
				redist = true;
				//commit right
				writeNode(right,tree);
			}
		} else if (pos>0) {
			//attempt redistribution on left sibling
			left = readNode(parent->childPage[pos-1],tree);
			if(left->numKeys > ufThresh) {
				for(i=node->numKeys-1 ; i >=0 ; i--) {
					//shift right in node
					copyValue(node->keys[i], node->keys[i+(ufThresh - node->numKeys)]);
					node->childRid[i+(ufThresh - node->numKeys)].page = node->childRid[i].page;
					node->childRid[i+(ufThresh - node->numKeys)].slot = node->childRid[i].slot;
				}
				for(i=0 ; i < (ufThresh - node->numKeys) ; i++) {
					//copy left to node
					copyValue(left->keys[left->numKeys - (ufThresh - node->numKeys)+i], node->keys[i]);
					node->childRid[i].page = left->childRid[left->numKeys - (ufThresh - node->numKeys)+i].page;
					node->childRid[i].slot = left->childRid[left->numKeys - (ufThresh - node->numKeys)+i].slot;
					left->keys[left->numKeys - (ufThresh - node->numKeys)+i]->v.intV = minINF;
					left->childRid[left->numKeys - (ufThresh - node->numKeys)+i].page=-1;
					left->childRid[left->numKeys - (ufThresh - node->numKeys)+i].slot=-1;
				}
				//update parent key
				copyValue(node->keys[0],parent->keys[pos-1]);
				node->numKeys = ufThresh;
				left->numKeys = left->numKeys - (ufThresh - node->numKeys);
				redist = true;
				//commit left
				writeNode(left, tree);
			}
		} else {
			//printf("DEL STEP 6.0 Redistribution Failed: Merge Attempt\n");
		}
		//If redistribution is possible, redistribute
		if(redist) {
			//commit parent , commit node
			btmData->numEntries = btmData->numEntries - 1;
			writeTreeInfo(tree);
			writeNode(parent,tree);
			writeNode(node,tree);
			return RC_OK;
		}
		//Else Merge with sibling,
		//if parent is root, merge and delete root else merge and back propagate
		//if(getRootPageNum(tree) == parent->nodeNum)
		if(pos < parent->numKeys) {
			//merge right sibling to node
			for(i=0; i < right->numKeys; i++) {
				copyValue(right->keys[i], node->keys[node->numKeys + i]);
				node->childRid[node->numKeys + i].page = right->childRid[i].page;
				node->childRid[node->numKeys + i].slot = right->childRid[i].slot;
				right->keys[i]->v.intV = minINF;
				right->childRid[i].page = -1;
				right->childRid[i].slot= -1;
			}
			node->numKeys = node->numKeys+right->numKeys;
			node->next = right->next;
			//remove key from parent 
			for(i = pos; i < parent->numKeys ; i++) copyValue(parent->keys[i+1],parent->keys[i]);
			for(i = pos+1; i < parent->numKeys+1 ; i++) parent->childPage[i] = parent->childPage[i+1];
		} else if (pos>0) {
			//merge left sibling to node
			for(i = node->numKeys -1; i>=0 ; i--) copyValue(node->keys[i],node->keys[i+left->numKeys]);
			for(i = node->numKeys -1; i>=0 ; i--) {
				node->childRid[i+left->numKeys].page = node->childRid[i].page;
				node->childRid[i+left->numKeys].slot = node->childRid[i].slot;
			} 
			for(i = 0 ; i<left->numKeys ; i++) {
				copyValue(left->keys[i],node->keys[i]);
				node->childRid[i].page = left->childRid[i].page;
				node->childRid[i].slot = left->childRid[i].slot;
			}
			node->numKeys = node->numKeys+left->numKeys;
			//remove key from parent 
			for(i = pos-1; i < parent->numKeys ; i++) copyValue(parent->keys[i+1],parent->keys[i]);
			for(i = pos; i < parent->numKeys+1 ; i++) parent->childPage[i]= parent->childPage[i+1];
		}
		writeNode(node,tree);
		parent->numKeys = parent->numKeys -1;
		btmData->numNodes = btmData->numNodes - 1;

		if(parent->nodeNum == getRootPageNum(tree)) {
			if(parent->numKeys==0) {
				//change root
				btmData->root = node;
				btmData->numNodes = btmData->numNodes -1 ;
			} else {
				writeNode(parent,tree);
				btmData->numEntries = btmData->numEntries - 1;
			}
			writeTreeInfo(tree);
		} else {
			writeNode(parent,tree);
			deleteBackPropagate(tree, parent->nodeNum);
		}
	}
	
	return RC_OK;
}
extern RC deleteBackPropagate(BTreeHandle *tree, int nodePage)
{
	int pos,i, ufThresh,fanOut;
	bool redist;
	Value *tmpVal;
	BTNode *node, *left, *right, *newRoot, *parent;
	BTreeMeta *btmData = (BTreeMeta *)tree->mgmtData;
	fanOut = getFanOut(tree);
	node = readNode(nodePage,tree);

	//if node is not in underflow , or node is root :  Done
	//if node is not root, and in underflow, check redistribution
	//if redistribution possible, redistribute
	//Else Merge with sibling, delete back propagate
	ufThresh = fanOut/2;
	if(fanOut%2) {
		ufThresh = (fanOut+1)/2;
	} 
	if((getRootPageNum(tree) == node->nodeNum) || (node->numKeys >= ufThresh)) return RC_OK;
	//If non root internal node is in underflow, check redistribution
	parent = readNode(node->parent,tree);
	pos=0;
	for(i=0; i < parent->numKeys+1; i++) {
		if(parent->childPage[i] == node->nodeNum) {
			pos=i;
			break;
		}
	}
	redist = false;
	if(pos < parent->numKeys) {
		//attempt redistribution on right sibling
		right = readNode(parent->childPage[pos+1],tree);
		if(right->numKeys > ufThresh) {
			for(i=0 ; i < right->numKeys+1 ; i++) {
				if(i < (ufThresh - node->numKeys)) {
					//copy right to node
					copyValue(right->keys[i], node->keys[node->numKeys+i]);
					node->childPage[node->numKeys+i]= right->childPage[i];
				} else {
					//shift node in right sibling
					copyValue(right->keys[i], right->keys[i-(ufThresh - node->numKeys)]);
					right->childPage[i-(ufThresh - node->numKeys)]= right->childPage[i];
				}
			}
			for(i=0; i < (ufThresh-node->numKeys) ; i++) {
				right->keys[right->numKeys-(ufThresh-node->numKeys)+i]->v.intV=minINF;
				right->childPage[right->numKeys-(ufThresh-node->numKeys)+i]=-1;
			}
			//update parent key
			copyValue(right->keys[0],parent->keys[pos]);
			node->numKeys = ufThresh;
			right->numKeys = right->numKeys - (ufThresh - node->numKeys);
			redist = true;
			//commit right
			writeNode(right,tree);
		}
	} else if (pos>0) {
		//attempt redistribution on left sibling
		left = readNode(parent->childPage[pos-1],tree);
		if(left->numKeys > ufThresh) {
			for(i=node->numKeys-1 ; i >=0 ; i--) {
				//shift right in node
				copyValue(node->keys[i], node->keys[i+(ufThresh - node->numKeys)]);
				node->childPage[i+(ufThresh - node->numKeys)]= node->childPage[i];
			}
			for(i=0 ; i < (ufThresh - node->numKeys) ; i++) {
				//copy left to node
				copyValue(left->keys[left->numKeys - (ufThresh - node->numKeys)+i], node->keys[i]);
				node->childPage[i]= left->childPage[left->numKeys - (ufThresh - node->numKeys)+i];
				left->keys[left->numKeys - (ufThresh - node->numKeys)+i]->v.intV = minINF;
				left->childPage[left->numKeys - (ufThresh - node->numKeys)+i]=-1;
			}
			//update parent key
			copyValue(node->keys[0],parent->keys[pos-1]);
			node->numKeys = ufThresh;
			left->numKeys = left->numKeys - (ufThresh - node->numKeys);
			redist = true;
			//commit left
			writeNode(left, tree);
		}
	} else {
		//printf("DELBACK STEP 4 Redistribution Failed: Merge Attempt\n");
	}
	//If redistribution is possible, redistribute
	if(redist) {
		//commit parent , commit node
		btmData->numEntries = btmData->numEntries - 1;
		writeTreeInfo(tree);
		writeNode(parent,tree);
		writeNode(node,tree);
		return RC_OK;
	}
	//Else Merge with sibling,
	//if parent is root, merge and delete root else merge and back propagate
	//if(getRootPageNum(tree) == parent->nodeNum)
	if(pos < parent->numKeys) {
		//merge right sibling to node
		for(i=0; i < right->numKeys; i++) {
			copyValue(right->keys[i], node->keys[node->numKeys + i]);
			node->childPage[node->numKeys + i]= right->childPage[i];
			right->keys[i]->v.intV = minINF;
			right->childPage[i] = -1;
		}
		node->numKeys = node->numKeys+right->numKeys;
		//remove key from parent 
		for(i = pos; i < parent->numKeys ; i++) copyValue(parent->keys[i+1],parent->keys[i]);
		for(i = pos+1; i < parent->numKeys+1 ; i++) parent->childPage[i]= parent->childPage[i+1];
	} else if (pos>0) {
		//merge left sibling to node
		for(i = node->numKeys -1; i>=0 ; i--) copyValue(node->keys[i],node->keys[i+left->numKeys]);
		for(i = node->numKeys -1; i>=0 ; i--) node->childPage[i+left->numKeys]= node->childPage[i];
		for(i = 0 ; i<left->numKeys ; i++) copyValue(left->keys[i],node->keys[i]);
		for(i = 0 ; i<left->numKeys ; i++) node->childPage[i]= left->childPage[i];

		node->numKeys = node->numKeys+left->numKeys;
		//remove key from parent 
		for(i = pos-1; i < parent->numKeys ; i++) copyValue(parent->keys[i+1],parent->keys[i]);
		for(i = pos; i < parent->numKeys+1 ; i++) parent->childPage[i]= parent->childPage[i+1];
	}
	writeNode(node,tree);
	parent->numKeys = parent->numKeys -1;
	btmData->numNodes = btmData->numNodes - 1;

	if(parent->nodeNum == getRootPageNum(tree)) {
		if(parent->numKeys==0) {
			//change root
			btmData->root = node;
			btmData->numNodes = btmData->numNodes -1 ;
		} else {
			writeNode(parent,tree);
			btmData->numEntries = btmData->numEntries - 1;
		}
		writeTreeInfo(tree);
	} else {
		writeNode(parent,tree);
		deleteBackPropagate(tree, parent->nodeNum);
	}
	return RC_OK;
}



extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	int firstLeaf = -1;
	int nodePageNum = -1;
	BT_ScanMeta *scanMeta = (BT_ScanMeta *)malloc(sizeof(BT_ScanMeta));

	(*handle) = (BT_ScanHandle *) malloc(sizeof(BT_ScanHandle));
	(*handle)->tree = tree;

	nodePageNum = getRootPageNum(tree);
	while(true) {
		//load node with nodePageNum
		scanMeta->currentLeafNode= readNode(nodePageNum, tree);
		//if node is leaf ,firstLeaf = node->nodeNum , break;
		if(scanMeta->currentLeafNode->isLeaf) {
			firstLeaf = scanMeta->currentLeafNode->nodeNum;
			break;
		} else {
			nodePageNum = scanMeta->currentLeafNode->childPage[0];
			freeNode(&scanMeta->currentLeafNode,tree);
		}
	}
	scanMeta->firstLeafPage = firstLeaf;
	scanMeta->currentLeafPage = firstLeaf;
	scanMeta->currentEntry=-1;
	scanMeta->currentLeafEntry=-1;
	(*handle)->mgmtData = scanMeta;
	//free(h);
	//free(page);
	return RC_OK;
}

extern RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	BT_ScanMeta *scanMeta = (BT_ScanMeta *) handle->mgmtData;
	RC ret = RC_OK;
	int numEntries;
	getNumEntries(handle->tree,&numEntries);
	if((scanMeta->currentEntry+1) >= numEntries) {
		ret =  RC_IM_NO_MORE_ENTRIES;
	} else {
		//intermediate entry;
		if(scanMeta->currentLeafEntry == scanMeta->currentLeafNode->numKeys - 1) {
			if(scanMeta->currentLeafNode->next == -1 ) return RC_IM_NO_MORE_ENTRIES;
			else {
				scanMeta->currentLeafPage = scanMeta->currentLeafNode->next;
				scanMeta->currentLeafEntry=0;
				//reload currnetLeafNode;
				freeNode(&scanMeta->currentLeafNode,handle->tree);
				scanMeta->currentLeafNode= readNode(scanMeta->currentLeafPage,handle->tree);
			}
		} else {
			scanMeta->currentLeafEntry= scanMeta->currentLeafEntry + 1;
		}
		scanMeta->currentEntry = scanMeta->currentEntry + 1;
		// set rid
		(*result) = (RID) scanMeta->currentLeafNode->childRid[scanMeta->currentLeafEntry];
	}
	return ret;
}

extern RC closeTreeScan (BT_ScanHandle *handle)
{
	BT_ScanMeta *scanMeta = (BT_ScanMeta *) handle->mgmtData;
	freeNode(&scanMeta->currentLeafNode, handle->tree);
	handle->tree = NULL;
	free(scanMeta);
	return RC_OK;
}


// debug and test functions
extern char *printTree (BTreeHandle *tree)
{
	char *result;
	int nodePosition = 0;
	result = printNode(tree, getRootPageNum(tree) , &nodePosition);
	return result;
}
extern char *printNode(BTreeHandle *tree,int pageNum, int *position) 
{
	int i,p, tmpPos;
	Value *k;
	RID r;
	VarString *result, *rem;
	char *remaining;
	BTNode *node ;
	node = readNode(pageNum, tree);

	MAKE_VARSTRING(result);
	MAKE_VARSTRING(rem);
	tmpPos = (*position);
	APPEND(result, "(%i)[", tmpPos);
	for(i = 0; i < node->numKeys; i++) 
	{
		tmpPos=tmpPos+1;
		k=node->keys[i];
		if(node->isLeaf) {
			r= node->childRid[i];
			APPEND(result, "%i.%i,%s%s",r.page,r.slot,serializeValue(k),(i==node->numKeys-1)? "":"," );
		} else {
			(*position) = (*position) + 1;
			APPEND(result, "%i,%s%s",(*position),serializeValue(k),(i==node->numKeys-1)? "":"," );
			p= node->childPage[i];
			if(p>0) {
				APPEND(rem, "%s",printNode(tree,p,position));
			}
		}
	}
	if(node->isLeaf) {
		if(node->next != -1) {
			APPEND(result, ",%i",((*position)+1) );
		}
	} else {
		(*position) = (*position) + 1;
		p= node->childPage[i];
		if(p>0) {
			APPEND(rem, "%s",printNode(tree,p,position));
		}
		APPEND(result, ",%i",((*position)));
	}
	
	APPEND(result, "]\n");
	GET_STRING(remaining, rem);
	APPEND(result, "%s",remaining);
	freeNode(&node,tree);
	RETURN_STRING(result);
}

extern char *serializeIdxInfo(BTreeHandle *tree) 
{
	VarString *result;
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData;
	MAKE_VARSTRING(result);
	//Create Meta Page
	APPEND(result, "NumEntries:%i\n", btmData->numEntries);
	APPEND(result, "NumNodes:%i\n", btmData->numNodes);
	APPEND(result, "FanOut:%i\n", btmData->fanOut);
	switch (tree->keyType)
	{
	    case DT_INT:
		APPEND(result, "KeyType:%s\n","INT");
		break;
	    case DT_FLOAT:
		APPEND(result, "KeyType:%s\n","FLOAT");
		break;
	    case DT_STRING:
		APPEND(result, "KeyType:%s\n","STRING");
		break;
	    case DT_BOOL:
		APPEND(result, "KeyType:%s\n","BOOL");
		break;
	}
	APPEND(result, "RootNode:%i\n", btmData->root->nodeNum);
	APPEND(result, "MaxPage:%i\n", btmData->maxPageNum);
  	RETURN_STRING(result);  
}
extern RC stringToIdxInfo(char *idxStr, int *nEntries, int * nNodes, int *fOut, DataType *kType, int *rootPage, int *maxPage) 
{
	char **idxComp, **subComp;
	char *delim;
	int splitCnt1,splitCnt2;
	delim="\n";	
	idxComp = strsplit(idxStr,delim,&splitCnt1);
	delim=":";
	//nEntries
	subComp = strsplit(idxComp[0],delim,&splitCnt2);
	(*nEntries)= atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);
	//nNodes
	subComp = strsplit(idxComp[1],delim,&splitCnt2);
	(*nNodes)= atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);
	//fOut
	subComp = strsplit(idxComp[2],delim,&splitCnt2);
	(*fOut)= atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);
	//kType
	subComp = strsplit(idxComp[3],delim,&splitCnt2);
	if(strcmp(subComp[1],"INT")==0) {
		(*kType)= DT_INT;
	} else if (strcmp(subComp[1],"FLOAT")==0) {
		(*kType)= DT_FLOAT;
	} else if (strcmp(subComp[1],"BOOL")==0) {
		(*kType)= DT_BOOL;
	} else if (strstr(subComp[1],"STRING") != NULL) {
		(*kType)= DT_STRING;
	}
	freeStrArr(subComp,splitCnt2);
	free(subComp);
	//rootPage
	subComp = strsplit(idxComp[4],delim,&splitCnt2);
	(*rootPage)= atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);
	//maxPage
	subComp = strsplit(idxComp[5],delim,&splitCnt2);
	(*maxPage)= atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);
	
	freeStrArr(idxComp, splitCnt1);
	free(idxComp);
	return RC_OK;
}

extern char *serializeNode(BTNode *node , int fanOut) 
{
	RID r;
	int ch;
	int i;
	VarString *result;
	MAKE_VARSTRING(result);
	//Create rootNode empty Page
	MAKE_VARSTRING(result);
	APPEND(result, "NodeNum:%i\n", node->nodeNum);
	APPEND(result, "Parent:%i\n", node->parent);
	APPEND(result, "NumKeys:%i\n", node->numKeys);
	APPEND(result, "IsLeaf:%i\n", node->isLeaf);
	APPEND(result, "Keys:[");
	for(i = 0; i < fanOut ; i++) {
		APPEND(result, "%s%s",serializeValue(node->keys[i]),(i==fanOut-1) ? "": ",");
	}
	APPEND(result, "]\n");
	APPEND(result, "Child:[");
	for(i = 0; i < fanOut+1 ; i++) {
		if(node->isLeaf) {
			r = node->childRid[i];
			APPEND(result, "%i.%i%s",r.page,r.slot,(i==fanOut) ? "": ",");
		} else {
			ch= node->childPage[i];
			APPEND(result, "%i%s",ch,(i==fanOut) ? "": ",");
		}
	}
	APPEND(result, "]\n");
	APPEND(result, "Next:%i\n",node->next);
  	RETURN_STRING(result);  
		
}
extern BTNode *stringToNode(char *nodeStr) 
{
	int i, nodeNum, parent,numKeys, fanOut;
	bool isLeaf;
	BTNode *node ;

	char **nodeComp, **subComp, **ridComp;
	char *delim,*delim1;
	int splitCnt1,splitCnt2,splitCnt3;
	delim ="\n";
	nodeComp = strsplit(nodeStr,delim,&splitCnt1);
	delim=":[,]";
	subComp = strsplit(nodeComp[0],delim,&splitCnt2);
	nodeNum = atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);

	subComp = strsplit(nodeComp[1],delim,&splitCnt2);
	parent = atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);

	subComp = strsplit(nodeComp[2],delim,&splitCnt2);
	numKeys = atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);

	subComp = strsplit(nodeComp[3],delim,&splitCnt2);
	isLeaf = atoi(subComp[1]) == 1;
	freeStrArr(subComp,splitCnt2);
	free(subComp);

	subComp = strsplit(nodeComp[4],delim,&splitCnt2);
	fanOut = splitCnt2 - 1;
	if(isLeaf) createNewNodeLeaf(&node, fanOut);
	else createNewNodeInternal(&node,fanOut);
	node->nodeNum = nodeNum;
	node->parent = parent;
	node->numKeys = numKeys;
	node->isLeaf = isLeaf;
	
	for(i=1;i<splitCnt2; i++) node->keys[i-1]->v.intV = atoi(subComp[i]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);

	subComp = strsplit(nodeComp[5],delim,&splitCnt2);
	if(node->isLeaf==1) {
		delim1=".";
		for(i=1;i<splitCnt2; i++) {
			ridComp = strsplit(subComp[i],delim1,&splitCnt3);
			node->childRid[i-1].page = atoi(ridComp[0]);
			node->childRid[i-1].slot= atoi(ridComp[1]);
			freeStrArr(ridComp, splitCnt3);
			free(ridComp);
		}
	} else {
		for(i=1;i<splitCnt2; i++) node->childPage[i-1] = atoi(subComp[i]);
	}
	freeStrArr(subComp,splitCnt2);
	free(subComp);

	subComp = strsplit(nodeComp[6],delim,&splitCnt2);
	node->next = atoi(subComp[1]);
	freeStrArr(subComp,splitCnt2);
	free(subComp);


	freeStrArr(nodeComp,splitCnt1);
	free(nodeComp);
	return node;
}
extern void createNewNodeInternal(BTNode **node,int fanOut) 
{
	int i;
	Value *dummy;
	(*node) = (BTNode *)malloc(sizeof(BTNode));
	(*node)->keys = (Value **)malloc(sizeof(Value *)*(1+fanOut)); 
	(*node)->childPage = (int *)malloc(sizeof(int)*(2+fanOut)); 
	for(i=0;i<fanOut+1; i++) {
		dummy = (Value *) malloc (sizeof(Value));
		dummy->dt=DT_INT;
		dummy->v.intV=minINF;
		(*node)->keys[i] = dummy;
	}
	for(i=0;i<=fanOut; i++) (*node)->childPage[i]=-1;
	(*node)->isLeaf=0;
	return;
}


extern void createNewNodeLeaf(BTNode **node,int fanOut) 
{
	int i;
	Value *dummy;
	(*node) = (BTNode *)malloc(sizeof(BTNode));
	(*node)->keys = (Value **)malloc(sizeof(Value *)*(1+fanOut)); 
	(*node)->childRid = (RID *)malloc(sizeof(RID)*(2+fanOut)); 
	for(i=0;i<fanOut+1; i++) {
		dummy = (Value *) malloc (sizeof(Value));
		dummy->dt=DT_INT;
		dummy->v.intV=minINF;
		(*node)->keys[i] = dummy;
	}
	for(i=0;i<=fanOut+1; i++) {
		(*node)->childRid[i].page=-1;
		(*node)->childRid[i].slot=-1;
	}
	(*node)->isLeaf=1;
	return;
}

extern void freeNode(BTNode **node,BTreeHandle * tree) 
{
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData;
	int fanOut = btmData->fanOut;
	int i;
	for(i =0; i < fanOut; i++) freeValue((*node)->keys[i]);
	if((*node)->isLeaf) free((*node)->childRid);
	else free((*node)->childPage);
	free((*node)->keys);
	free(*node);
	return;
}
extern void copyValue(Value *fromValue, Value *toValue) 
{
	toValue->dt = fromValue->dt;
	toValue->v.intV = fromValue->v.intV;
	return;
}
extern BTNode *readNode( int nodePage,BTreeHandle *tree) {
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData;
	BTNode *node;
	BM_PageHandle *h;
	char *page = (char*) malloc(PAGE_SIZE);
	h = MAKE_PAGE_HANDLE();
	pinPage(btmData->bm,h,nodePage);
	strncpy(page,h->data,PAGE_SIZE);
	unpinPage(btmData->bm,h);
	node= stringToNode(page);
	free(h);
	free(page);
	return node;
}
extern void writeNode(BTNode *node,BTreeHandle *tree) {
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData;
	BM_PageHandle *h;
	h = MAKE_PAGE_HANDLE();
	pinPage(btmData->bm,h,node->nodeNum);
	sprintf(h->data, "%s", serializeNode(node,btmData->fanOut));
	markDirty(btmData->bm,h);
	unpinPage(btmData->bm,h);
	free(h);
	return;	
}
extern void writeTreeInfo(BTreeHandle *tree) {
	BTreeMeta *btmData = (BTreeMeta *) tree->mgmtData;
	BM_PageHandle *h;
	h = MAKE_PAGE_HANDLE();
	pinPage(btmData->bm,h,0);
	sprintf(h->data, "%s", serializeIdxInfo(tree));
	markDirty(btmData->bm,h);
	unpinPage(btmData->bm,h);
	free(h);
	return;	
}
