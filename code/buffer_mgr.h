#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"
#include "list.h"
#include "storage_mgr.h"

// Replacement Strategies
/**
 * An enum to capture different replacement strategies
 */
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
/**
 * PageNumber is of int type
 */
typedef int PageNumber;
#define NO_PAGE -1

/**
 * BM_BufferPool creates an in memory pool of pages which are read using storage manager. 
 * The addition and deletion of pages in the pool is done based on the value of "strategy" 
 * All the pages are stored in mgmt data using a BM_MetaList struct object. 
 */
typedef struct BM_BufferPool {
  /*@{*/
  /** contains the name of the database file */
  char *pageFile; 
  /** contains the total number of pages currently in the pool */
  int numPages; 
  /** specifies the order of addition and deletion of pages */
  ReplacementStrategy strategy; 
  /** use this one to store the bookkeeping info your buffer  manager needs for a buffer pool */
  void *mgmtData; 
  /*@}*/
} BM_BufferPool;

/**
 * BM_PageHandle budles a PageNumber (int) pageNum field along with its page contents in field "data"
 */
typedef struct BM_PageHandle {
  /*@{*/
  /** denotes the page number for which the contents are stored in "data" */ 
  PageNumber pageNum; 
  /** contains data corresponding to page number pageNum */
  char *data; 
  /*@}*/
} BM_PageHandle;

/**
 * This structures combines some basic meta information to a pageHandle, that allows us to maintain some book-keeping
 * information for a given pageHandle
 */
typedef struct BM_MetaPage {
  /*@{*/
  /** This variable takes a value of 1 when a page is marked dirty, and it has a 0 value otherwise */
  int dirtyFlag; 
  /** This variable maitains a count of the number times this page has been pinned (and still not unpinned). */	
  int fixCount; 
  /** This contains the main data for the page */
  BM_PageHandle *pHandle; 
  /*@}*/
} BM_MetaPage;

/**
 * This structure contains some book keeping information for the buffer pool, along with the pages in the pool.
 */
typedef struct BM_MetaList {
  /*@{*/
  /** This variable maintains a count of Read IO since the time a buffer pool initiated */
  int numReadIO; 
  /** This variable maintains a count of Write IO since the time buffer pool is initiated */
  int numWriteIO; 
  /** This variable is actually a linked list of pages in the pool, the data in the node of this linked list is of type BM_MetaPage */
  linkedList *pList; 
  /*@}*/
} BM_MetaList;

// convenience macros
#define MAKE_POOL()					\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

#define MAKE_META_PAGE()				\
  ((BM_MetaPage *) malloc (sizeof(BM_MetaPage)))

#define MAKE_META_LIST()				\
  ((BM_MetaList *) malloc (sizeof(BM_MetaList)))

// Buffer Manager Interface Pool Handling

/**
 * Initializes a buffer pool, and allocates memory to pages 
 * @param[in] bm The buffer pool to initialize.
 * @param[in] pageFileName The name of the pageFile for which we are initializing this instance of buffer manager
 * @param[in] numPages The number of pages that we want to keep in the pool 
 * @param[in] strategy Specifies which paging strategy to use adding and deleting pages from the pool 
 * @param[in] stratData contains additional information for strategy 
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData);

/**
 * Deallocates memory allotted the contents of the buffer pool 
 * @param[in] bm The buffer pool that we want to de allocate.
 */
RC shutdownBufferPool(BM_BufferPool *const bm);

/**
 * Iterates through all the pages of the bufferpool, checks if the page is dirty and if it is, then it writes the pages to disk using storage_mgr function writeBlock.
 * @param[in] bm The buffer pool to force flush on disk
 */
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages

/**
 * Tags a page as dirty, signifying that the current contents of the page might be different from what resides on disk
 * @param[in] bm The buffer pool which contains this page
 * @param[in] page The BM_PageHandle of the page that we want to mark as dirty.
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);

/**
 * Reduces the fixCount of the page by 1 (if fixCount >0).  This signifies that one of the processes is no longer accessing this page
 * @param[in] bm The buffer pool which contains this page
 & @param[in] page The handle of the page that we want to unpin.
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);

/**
 * If the page is dirty, this function forces the page to be written on disk
 * @param[in] bm The buffer pool which contains this page
 * @param[in] bm The handle of the page that we want to force on disk 
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);

/**
 * Increments the fixCount of the page by 1. This signifies that one more process is now accesesing this page in the buffer pool
 * @param[in] bm The buffer pool which contains this page
 * @param[out] page The handle of the page indicated by pageNum
 * @param[in] pageNum The page number that we want to pin. 
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum);
//Paging Strategy Specific Functions 

/**
 * This function is called from within pinPage. The pool is created using a liked list (linkedList). This function performs addition as well as deletion of pages based on fifo logic. If the pool has empty slots, this function only adds pages. If the pool has no empty slots, the first a deletion is performed and then an addition is performed. The additions are done at the tail of the linked list. The deletions are done at the head. This keeps the linked list sorted in fifo order. After deletion of a node all its memory is deallocated.
 * @param[in] bm The buffer pool which contains this page
 * @param[in] pageNum The page number that we want to add to pool. 
 */
RC fifoAddPage(BM_BufferPool *const bm,  const PageNumber pageNum) ;

/**
 * This function is called from within pinPage. The pool is created using a liked list (linkedList). This function performs addition as well as deletion of pages based on LRU logic. If the pool has empty slots, this function only adds pages. If the pool has no empty slots, the first a deletion is performed and then an addition is performed. The additions are done at the tail of the linked list. The deletions are done at the head. If the page is already present in the list, even then the page is first removed from the list and then added at the tail. This keeps the linked list sorted in LRU order (with the page at head always being the least recently used). After deletion of a node all its memory is deallocated.
 * @param[in] bm The buffer pool which contains this page
 * @param[in] pageNum The page number that we want to add to pool. 
 */
RC  lruAddPage(BM_BufferPool *const bm,  const PageNumber pageNum) ;

// Statistics Interface

/**
 * Returns an array of page numbers currently contained in the buffer pool
 * @param[in] bm The buffer pool which we are querying 
 */
PageNumber *getFrameContents (BM_BufferPool *const bm);

/**
 * Returns an array of type bool of length = size of buffer pool. Each bool entry of this array is 1 if the corresponding page is dirty, else it is 0.
 * @param[in] bm The buffer pool which we are querying 
 */
bool *getDirtyFlags (BM_BufferPool *const bm);

/**
 * Returns an array of length = size of buffer pool. Each integer entry contains the fixCount of the corresponding page in buffer pool. 
 * @param[in] bm The buffer pool which we are querying 
 */
int *getFixCounts (BM_BufferPool *const bm);

/**
 * Returns the number of times the buffer pool has had Read IO since its initialization 
 * @param[in] bm The buffer pool which we are querying 
 */
int getNumReadIO (BM_BufferPool *const bm);

/**
 * Returns the number of times the buffer pool has had Write IO since its initialization 
 * @param[in] bm The buffer pool which we are querying 
 */
int getNumWriteIO (BM_BufferPool *const bm);

#endif
