#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

// structure for accessing btrees
/**
 * This structure stores a btree with name idxId. The btree related data is stored in mgmtData. 
 */
typedef struct BTreeHandle {
  /*@{*/
  /** The data type of the btree key. Currently only int values are supported.*/
  DataType keyType;
  /** Name of the btree index*/
  char *idxId;
  /** Stores Btree related data,  BTreeMeta */
  void *mgmtData;
  /*@}*/
} BTreeHandle;

/**
 * This structure represents an individual node of BTree
 */
typedef struct BTNode {
	/*@{*/
	/** The page number (on disk) of this node */
	int nodeNum; //page num of node;
	/** The page number of parent node */
	int parent;
	/** Number of keys in this node */
	int numKeys;
	/** Boolean value. If true, this node is leaf, else node is an internal node */
	bool isLeaf;
	/** This variable contains array of keys */
	Value **keys;
	/** This variable contains array of child pages. Applicable only of internal nodes. */
	int * childPage;
	/** This variable contains array of child recordIds . Applicable only for leaf nodes */
	RID * childRid; 
	/** Page number of the next node */
	int next; 
  	/*@}*/
} BTNode;

/**
 * This structure stores the details of a BTree. It is stored in BTreeHandle->mgmtData 
 */
typedef struct BTreeMeta {
	/*@{*/
	/** Number of RIDs in BTree */
	int numEntries;
	/** Number of Nodes in BTree */
	int numNodes;
	/** The fanOut of the BTree (Max number of keys that a node can have) */
	int fanOut;
	/** Pointer to Root Node */
	BTNode *root;
	/** Pointer to Buffer manager, to be used of accessing node pages */
	BM_BufferPool *bm;
	/** The maximum value of any page number in a btree index structure  */
	int maxPageNum;
  	/*@}*/
} BTreeMeta ; 


/**
 * This structure stores the details of a tree scan operation.
 */
typedef struct BT_ScanHandle {
  /*@{*/
  /** The BTree to be scanned */
  BTreeHandle *tree;
  /** Contains meta data related to scan. The meta data is stored using structure BT_ScanMeta */
  void *mgmtData;
  /*@}*/
} BT_ScanHandle;

/**
 * This structure stores an intermediate stage of the scan operation. It is initialized to represent the first element in BTree (sorted ascending by key).
 */
typedef struct BT_ScanMeta {
	/*@{*/
	/** Page number of first leaf node. */
	int firstLeafPage;
	/** Page number of current lead node being scanned. */
	int currentLeafPage;
	/** Contains the total number of entries scanned so far. */
	int currentEntry;
	/** Contains the number of entries scanned so far in the currentLeafNode. */
	int currentLeafEntry;
	/** Pointer to current leaf node being scanned. This is initialized to firstLeafNode at the initiation of scan */
	BTNode *currentLeafNode;
  	/*@}*/
} BT_ScanMeta;

// init and shutdown index manager
/**
 * This function initializes the index manager. 
 * @param[in] mgmtData Contains nothing in my implementation.
 */
extern RC initIndexManager (void *mgmtData);
/**
 * This function closes the index manager. Contains nothing in my implementation. 
 */
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
/**
 * This function creates an empty BTree of name idxId which has an empty node as root node. 
 * @param[in] idxId Name of the BTree index file. 
 * @param[in] DataType The data type of the key of the btree index. 
 * @param[in] n The fanOut of the tree to be created. 
 */
extern RC createBtree (char *idxId, DataType keyType, int n);
/**
 * This function reads idxId file and stores the corresponding BTree in tree. 
 * @param[out] tree Pointer to pointer to tree being read from file. 
 * @param[in] idxId Name of the file on disk, which contains the  BTree. (The buffer manager is deallocated as well).
 */
extern RC openBtree (BTreeHandle **tree, char *idxId);
/**
 * This function Closes a BTree tree, by deallocating and NULL-resetting of the components of the handle. 
 * @param[in] tree The tree that needs to be closed. (The buffer manager is deallocated as well).
 */
extern RC closeBtree (BTreeHandle *tree);
/**
 * This function deletes a BTree index file. 
 * @param[in] idxId This is the name of the BTree to be deleted. 
 */
extern RC deleteBtree (char *idxId);

// access information about a b-tree
/**
 * This function returns the number of nodes stored in a BTree.. 
 * @param[in] tree The tree whose number of nodes is being queried. 
 * @param[out] result Used to return the number of nodes contained in the btree. 
 */
extern RC getNumNodes (BTreeHandle *tree, int *result);
/**
 * This function returns the number of rids stored in a BTree. 
 * @param[in] tree The tree whose numEntries is being queriesd. 
 * @param[out] result Used to return the number of entries contained in the btree. 
 */
extern RC getNumEntries (BTreeHandle *tree, int *result);
/**
 * This function returns the key type of the BTree index key . 
 * @param[in] tree The tree whose index key type is being queried. 
 */
extern RC getKeyType (BTreeHandle *tree, DataType *result);
/**
 * This function returns the page number of the root node of tree. 
 * @param[in] tree The tree whose root page number is being queried. 
 */
extern int getRootPageNum(BTreeHandle *tree);
/**
 * This function returns the fanOut of the tree. 
 * @param[in] tree The tree whose fanOut is being queried. 
 */
extern int getFanOut(BTreeHandle *tree);
// index access
/**
 * This function is used to find a key in a tree and return its corresponding rid in result. Internally it calls recursiveFindKey with root node. 
 * @param[in] tree The tree in which we are trying to find. 
 * @param[in] key The key which we are trying to find. 
 * @param[out] result  Returns the rid corresponding to key, if key is found in the tree. 
 */
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
/**
 * This function recursively finds a key in tree, and returns its value in result.. 
 * @param[in] tree The tree in which we are trying to find. 
 * @param[in] key The key which we are trying to find. 
 * @param[out] result  Returns the rid corresponding to key, if key is found in the tree. 
 * @param[in] nodePage The page number of node from which we are to begin the search. 
 */
extern RC recursiveFindKey (BTreeHandle *tree, Value *key, RID *result,int nodePage);
/**
 * This function is used to insert a key, rid pair into a btree. Internally it calls recursiveInsertKey starting from root node.  
 * @param[in] tree The tree in which we want to insert. 
 * @param[in] key The key to be inserted. 
 * @param[in] rid The rid to be inserted. 
 */
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
/**
 * This function inserts a key, rid pair in a BTree subtree starting at node with page number nodePage. 
 * @param[in] tree The tree in which we want to insert. 
 * @param[in] key The key that we want to insert. 
 * @param[in] rid The rid that we want to insert in subtree starting at node with page number nodePage. 
 * @param[in] nodePage Page number of the node where the subtree begins. The rid that we want to insert. 
 */
extern RC recursiveInsertKey (BTreeHandle *tree, Value *key, RID rid, int nodePage);
/**
 * This function back propagates any splits that happen on the leaf node, back up to the root node. 
 * @param[in] tree The tree in which the insert operation is to be back propagated. 
 * @param[in] key The key to be inserted in the node (as per back propagation need). 
 * @param[in] newPage The page corresponding to key that is to be entered. 
 * @param[in] parentPage The pageNumber of node in which [key,newPage] is to be inserted. 
 */
extern RC insertBackPropagate(BTreeHandle *tree, Value* key, int newPage, int parentPage);
/**
 * This function is used to delete a key in a BTree. Internally it calls recursiveDeleteKey with root node.
 * @param[in] tree The tree from which the key is to be deleted. 
 * @param[in] key The key that is to be deleted from the tree. 
 */
extern RC deleteKey (BTreeHandle *tree, Value *key);
/**
 * This function recursively goes down to the leaf to delete a given key,rid from the btree. If needed it calls deleteBackPropagate to correct for underflows starting from leaf up untill the root node. 
 * @param[in] tree The tree from which key is to be deleted. 
 * @param[in] key The key to be deleted. 
 * @param[in] nodePage The node from which the backpropagation of underflow is to be started. 
 */
extern RC recursiveDeleteKey (BTreeHandle *tree, Value *key, int nodePage);
/**
 * This function does the back propagation of underflow during a delete operation. 
 * @param[in] tree The tree in which the deletion operation is being done. 
 * @param[in] nodePage The page number of node from which we start checking for underflow. 
 */
extern RC deleteBackPropagate(BTreeHandle *tree, int nodePage);
/**
 * This function initializes a scan handle with details of a btree. The scan handle acts as an iterator. 
 * @param[in] tree The BTree to be scanned. 
 * @param[out] handle The scan handle that is to be initialized at the first element of the tree. 
 */
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
/**
 * This function iterates on each entry of the btree in a sorted order. 
 * @param[in] handle The scan handle which contains the current position within the tree. 
 * @param[out] result The next recordId in tree. 
 */
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
/**
 * This function deallocates and (NULL reset) the elements of a scan handle. This is to be called at the end of a scan operation. 
 * @param[in] handle The scan handle  that is to be closed.
 */
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
/**
 * This function is used to print a tree. Internally it uses printNode function to print the subtree of root node. 
 * @param[in] tree The tree to be printed. 
 */
extern char *printTree (BTreeHandle *tree);
/**
 * This function recursively travels in a pre-order depth first order and prints the subtree starting from node with page number pageNum. 
 * @param[in] tree The tree to be printed. 
 * @param[in] pageNum  The page number of node whose subtree is to be printed.
 * @param[inout] position This variables maintains the pre-order depth first order of each node through the recursive function calls. 
 */
extern char *printNode(BTreeHandle *tree, int pageNum, int *position);
/**
 * This function parses the string of first page of an index file, and returns all the parameters of the index accordingly. 
 * @param[in] idxStr The string to be parsed. 
 * @param[out] nEntries The number of entries in the BTree. 
 * @param[out] nNodes The number of node in the BTree. 
 * @param[out] fOut The fanOut of the BTree. 
 * @param[out] kTypoe The DataType of the key for this BTree. 
 * @param[out] rootPage The page number of the root node of the BTree. 
 * @param[out] maxPage The maximum page number that any node has in the  BTree. 
 */
extern RC stringToIdxInfo(char *idxStr, int *nEntries, int * nNodes, int *fOut, DataType *kType, int *rootPage, int *maxPage);
/**
 * This function creates the string equivalent of the tree meta info. This string is supposed to be the first page of any index file. 
 * @param[in] tree The tree for which we are creating the tree info string . 
 */
extern char *serializeIdxInfo(BTreeHandle *tree);
/**
 * This function converts a node to an equivalent string representation, which can be potentially written to disk. 
 * @param[in] node The node to be converted to string. 
 * @param[in] n The fanOut of the tree to which this node belongs.
 */
extern char *serializeNode(BTNode *node,int n);
/**
 * This function reads a node page and converts it into a node object. 
 * @param[in] nodeStr Node string as read from a node page. This node string will be converted to a node object. 
 */
extern BTNode *stringToNode(char *nodeStr);
/**
 * This function is used to read a node from a tree (on disk). 
 * @param[in] node The node to be written. 
 * @param[in] tree The tree to which the node is to be written. (Also contains the buffermanager for disk IO ) 
 */
extern BTNode *readNode( int nodePage, BTreeHandle *tree);
/**
 * This function writes a node to disk. For writing to disk, the buffer manager bm (in BTreeMeta, in BTreeHandle) is used. 
 * @param[in] node The node to be written. 
 * @param[in] tree The tree to which the node is to be written. (Also contains the buffermanager for disk IO ) 
 */
extern void writeNode(BTNode *node, BTreeHandle *tree);
/**
 * This function  saves the meta info of a given tree on page 0 of a BTree index page. 
 * @param[in] tree The tree whose meta info is to be saved. 
 */
extern void writeTreeInfo(BTreeHandle *tree);
/**
 * This function . 
 * This function allocates memory and returns a pointer to a node structure. It allocates memory to node->childPage and node->keys  
 * @param[in] node Pointer to pointer to node to be allocated. 
 * @param[in] fanOut the fanOut of tree for which this node is being created . 
 */
extern void createNewNodeInternal(BTNode **node, int fanOut);
/**
 * This function allocates memory and returns a pointer to a node structure. It allocates memory to node->childRid and node->keys  
 * @param[in] node Pointer to pointer to node to be allocated. 
 * @param[in] fanOut the fanOut of tree for which this node is being created . 
 */
extern void createNewNodeLeaf(BTNode **node, int fanOut);
/**
 * This function cleans up all the associated memory allocations to a node . 
 * @param[in] node Pointer to Pointer to node to be freed. 
 * @param[in] tree Thre tree to which the node belongs. (Needed for fanOut). 
 */
extern void freeNode(BTNode **node, BTreeHandle *tree);
/**
 * This function copies one Value struct onto another. 
 * @param[in] fromValue Value to copy. 
 * @param[out] toValue Copied Value (copied from fromValue). 
 */
extern void copyValue(Value *fromValue, Value *toValue);

#endif // BTREE_MGR_H
