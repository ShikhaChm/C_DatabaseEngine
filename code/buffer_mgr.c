#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "dberror.h"
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
	int i ;
	BM_MetaPage *mPage ;
	BM_MetaList *mList ;
	
	bm->pageFile = strdup(pageFileName);
	bm->numPages = numPages;
	bm->strategy = strategy;

	mList = MAKE_META_LIST();
	mList->numReadIO=0;
	mList->numWriteIO=0;
	mList->pList = listCreate();
	for (i = 0 ; i < numPages; i++ ) {
		//Create and insert MetaPage
		mPage = MAKE_META_PAGE();
		mPage->dirtyFlag=0;
		mPage->fixCount=0;
		mPage->pHandle = MAKE_PAGE_HANDLE();
		mPage->pHandle->pageNum = NO_PAGE;
		mPage->pHandle->data = (char *) malloc(PAGE_SIZE);
		listPush(mList->pList, mPage);
	}
	bm->mgmtData = mList;
	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int hasPinnedPages;
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
  	SM_FileHandle fh;

	mList = (BM_MetaList *)bm->mgmtData;

	hasPinnedPages=0;
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		if(mPage->fixCount > 0) {
			hasPinnedPages = 1;
			return RC_BM_POOL_HAS_PINNED_PAGES;
		}
	}
	//Write Page To Disk
	openPageFile (bm->pageFile, &fh);
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		if(mPage->dirtyFlag == 1) { 
			//printf("pgNo=%i fxCnt=%i  drty=%i data=%s\n", mPage->pHandle->pageNum, mPage->fixCount, mPage->dirtyFlag, mPage->pHandle->data);
			writeBlock (mPage->pHandle->pageNum, &fh, mPage->pHandle->data);	
			mList->numWriteIO +=1;
			mPage->dirtyFlag=0;
			
		}
	}
	closePageFile (&fh);
	
	//	free(bm);
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		free(mPage->pHandle->data);
		free(mPage->pHandle);
		free(mPage);
	}
	listDestroy(mList->pList);
	free(mList);
	free(bm->pageFile);
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{	
	//forceFlushPool causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.

	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
  	SM_FileHandle fh;

	mList = (BM_MetaList *)bm->mgmtData;
	//Write Page To Disk
	openPageFile (bm->pageFile, &fh);
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		if((mPage->dirtyFlag == 1) && (mPage->fixCount == 0 ))  { 
			writeBlock (mPage->pHandle->pageNum, &fh, mPage->pHandle->data);	
			mList->numWriteIO +=1;
			mPage->dirtyFlag=0;
		}
	}
	closePageFile (&fh);

	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
	mList = (BM_MetaList *)bm->mgmtData;

	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		if(mPage->pHandle->pageNum == page->pageNum) {
			mPage->dirtyFlag=1;
			break;
		}
	}

	return RC_OK;
}


RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
	mList = (BM_MetaList *)bm->mgmtData;
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		if(mPage->pHandle->pageNum == page->pageNum) {
			if(mPage->fixCount > 0) mPage->fixCount = mPage->fixCount - 1;
			break;
		}
	}
	return RC_OK;
}


RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
  	SM_FileHandle fh;

	mList = (BM_MetaList *)bm->mgmtData;
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		if(mPage->pHandle->pageNum == page->pageNum) {
			if((mPage->fixCount == 0) && (mPage->dirtyFlag == 1) ) {
					openPageFile (bm->pageFile, &fh);
					writeBlock (mPage->pHandle->pageNum, &fh, mPage->pHandle->data);	
					closePageFile (&fh);
					mList->numWriteIO +=1;
					mPage->dirtyFlag=0;
			} 
			break;
		}
	}
	return RC_OK;
}

//RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
//{
//	int i, pageFault,emptySlots ,retCode ; 
//	SM_FileHandle fh;
//	SM_PageHandle ph;
//
//	BM_MetaList *mList;
//	BM_MetaPage *mPage;
//	linkedListNode *pNode;
//	mList = (BM_MetaList *)bm->mgmtData;
//	pageFault = 1;
//	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
//		mPage = (BM_MetaPage *) pNode->value ; 
//		if (mPage->pHandle->pageNum == pageNum) { 
//			pageFault =0; 
//			break ; 
//		}
//	}
//	if(pageFault) {
//		ph = (SM_PageHandle) malloc(PAGE_SIZE);
//		//read from disk 
//		openPageFile (bm->pageFile, &fh);
//		if(pageNum >= fh.totalNumPages  ) ensureCapacity((pageNum+1),&fh); 
//		readBlock(pageNum, &fh, ph);
//		closePageFile (&fh);
//		mList->numReadIO +=1;
//		if(bm->strategy == RS_FIFO) {
//			retCode=fifoAddPage(bm,ph,pageNum);	
//		} else if(bm->strategy == RS_LRU) {
//			retCode=lruAddPage(bm,ph,pageNum);	
//		} else 
//			retCode= fifoAddPage(bm,ph,pageNum);	
//		if (retCode == RC_OK) {
//			for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
//				mPage = (BM_MetaPage *) pNode->value ; 
//				if(mPage->pHandle->pageNum == pageNum) break;
//			}
//			page->pageNum = pageNum;
//			page->data = mPage->pHandle->data;
//		} else return retCode;
//	} else {
//		mPage->fixCount = mPage->fixCount + 1;
//		page->pageNum   = pageNum;
//		page->data      = mPage->pHandle->data;
//	}
//	return RC_OK;
//}
//RC fifoAddPage(BM_BufferPool *const bm, SM_PageHandle  page , const PageNumber pageNum)  {
//		int emptySlots;
//		int hasUnpinnedPage;
//
//		BM_MetaList *mList;
//		BM_MetaPage *mPage;
//		linkedListNode *pNode;
//		SM_FileHandle fh;
//
//		mList = (BM_MetaList *)bm->mgmtData;
//		//Check if have empty frames
//		emptySlots=0;
//		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
//				mPage = (BM_MetaPage *) pNode->value ; 
//				if(mPage->pHandle->pageNum == NO_PAGE) {
//					emptySlots=1;
//					break;
//				}
//		}
//		if(!emptySlots) {
//			// remove some page and add new page as per the strategy
//			// remove the last non pinned page
//			hasUnpinnedPage = 0;
//			for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
//				mPage = (BM_MetaPage *) pNode->value ; 
//				if(mPage->fixCount == 0) {
//					hasUnpinnedPage=1;
//					break;
//				}
//			}
//			if(hasUnpinnedPage) {
//				listRemove(mList->pList,pNode);
//				if((mPage->fixCount==0) && (mPage->dirtyFlag==1) ) {
//						openPageFile (bm->pageFile, &fh);
//						writeBlock (mPage->pHandle->pageNum, &fh, mPage->pHandle->data);	
//						closePageFile (&fh);
//						mList->numWriteIO +=1;
//						mPage->dirtyFlag= 0;
//				}
//				listAddToEnd(mList->pList, mPage);
//			} else return RC_BM_FIFO_CANNOT_REMOVE_PINNED;
//		}
//		mPage->fixCount = 1;
//		mPage->dirtyFlag= 0;
//		mPage->pHandle->pageNum = pageNum;
//		mPage->pHandle->data = page;
//
//		return RC_OK;
//}



RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	int i, retCode ; 

	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
	mList = (BM_MetaList *)bm->mgmtData;

	//read from disk 
	if(bm->strategy == RS_FIFO) {
		retCode=fifoAddPage(bm,pageNum);	
	} else if(bm->strategy == RS_LRU) {
		retCode=lruAddPage(bm,pageNum);	
	} else 
		retCode= fifoAddPage(bm,pageNum);	
	if (retCode == RC_OK) {
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
			mPage = (BM_MetaPage *) pNode->value ; 
			if(mPage->pHandle->pageNum == pageNum) break;
		}
		page->pageNum = pageNum;
		page->data = mPage->pHandle->data;
	} else return retCode;
	return RC_OK;
}

//Paging Strategy Specific Functions
RC fifoAddPage(BM_BufferPool *const bm, const PageNumber pageNum)  {
		int emptySlots,pageFault;
		int hasUnpinnedPage;

		BM_MetaList *mList;
		BM_MetaPage *mPage, *delPage;
		linkedListNode *pNode;
		SM_FileHandle fh;
		SM_PageHandle ph;

		mList = (BM_MetaList *)bm->mgmtData;
		//Check if the pool already has it 
		pageFault = 1;
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
			mPage = (BM_MetaPage *) pNode->value ; 
			if (mPage->pHandle->pageNum == pageNum) { 
				pageFault =0; 
				break ; 
			}
		}
		if(!pageFault) {
			mPage->fixCount = mPage->fixCount + 1;
			return RC_OK;
		}

		ph = (SM_PageHandle) malloc(PAGE_SIZE);
		openPageFile (bm->pageFile, &fh);
		if(pageNum >= fh.totalNumPages  ) ensureCapacity((pageNum+1),&fh); 
		readBlock(pageNum, &fh, ph);
		closePageFile (&fh);
		mList->numReadIO +=1;

		//Check if have empty frames
		emptySlots=0;
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
				mPage = (BM_MetaPage *) pNode->value ; 
				if(mPage->pHandle->pageNum == NO_PAGE) {
					emptySlots=1;
					break;
				}
		}
		if(emptySlots) {
				mPage->fixCount = 1;
				mPage->dirtyFlag= 0;
				mPage->pHandle->pageNum = pageNum;
				free(mPage->pHandle->data);
				mPage->pHandle->data = ph;
				return RC_OK;
		}
		// remove some page and add new page as per the strategy
		// remove the last non pinned page
		hasUnpinnedPage = 0;
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
				mPage = (BM_MetaPage *) pNode->value ; 
				if(mPage->fixCount == 0) {
						hasUnpinnedPage=1;
						break;
				}
		}
		if(hasUnpinnedPage) {
				listRemove(mList->pList,pNode);
				if((mPage->fixCount==0) && (mPage->dirtyFlag==1) ) {
						openPageFile (bm->pageFile, &fh);
						writeBlock (mPage->pHandle->pageNum, &fh, mPage->pHandle->data);	
						closePageFile (&fh);
						mList->numWriteIO +=1;
						mPage->dirtyFlag= 0;
				}
				listAddToEnd(mList->pList, mPage);
				mPage->fixCount = 1;
				mPage->pHandle->pageNum = pageNum;
				free(mPage->pHandle->data);
				mPage->pHandle->data = ph;
				return RC_OK;
		} else return RC_BM_ALL_PAGES_PINNED;
}

RC lruAddPage(BM_BufferPool *const bm, const PageNumber pageNum)  {
		int emptySlots,pageFault,fix;
		int hasUnpinnedPage;

		BM_MetaList *mList;
		BM_MetaPage *mPage;
		linkedListNode *pNode;
		SM_FileHandle fh;
		SM_PageHandle ph;

		mList = (BM_MetaList *)bm->mgmtData;
		//Check if the pool already has the page 
		pageFault = 1;
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
			mPage = (BM_MetaPage *) pNode->value ; 
			if (mPage->pHandle->pageNum == pageNum) { 
				pageFault =0; 
				break ; 
			}
		}
		if(!pageFault) {
			fix = mPage->fixCount + 1;
			ph = strdup(mPage->pHandle->data);
			listRemove(mList->pList, pNode);
			mPage->pHandle->pageNum = NO_PAGE;
			listAddToEnd(mList->pList, mPage);
		} else {
			ph = (SM_PageHandle) malloc(PAGE_SIZE);
			openPageFile (bm->pageFile, &fh);
			if(pageNum >= fh.totalNumPages  ) ensureCapacity((pageNum+1),&fh); 
			readBlock(pageNum, &fh, ph);
			closePageFile (&fh);
			mList->numReadIO +=1;
			fix=1;
		}

		//Check if have empty frames
		emptySlots=0;
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
				mPage = (BM_MetaPage *) pNode->value ; 
				if(mPage->pHandle->pageNum == NO_PAGE) {
					emptySlots=1;
					break;
				}
		}
		if(emptySlots) {
				mPage->fixCount = fix;
				mPage->dirtyFlag= 0;
				mPage->pHandle->pageNum = pageNum;
				free(mPage->pHandle->data);
				mPage->pHandle->data = ph;
				return RC_OK;
		}
		// remove some page and add new page as per the strategy
		// remove the last non pinned page
		hasUnpinnedPage = 0;
		for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
				mPage = (BM_MetaPage *) pNode->value ; 
				if(mPage->fixCount == 0) {
						hasUnpinnedPage=1;
						break;
				}
		}
		if(hasUnpinnedPage) {
				listRemove(mList->pList,pNode);
				if((mPage->fixCount==0) && (mPage->dirtyFlag==1) ) {
						openPageFile (bm->pageFile, &fh);
						writeBlock (mPage->pHandle->pageNum, &fh, mPage->pHandle->data);	
						closePageFile (&fh);
						mList->numWriteIO +=1;
						mPage->dirtyFlag= 0;
				}
				listAddToEnd(mList->pList, mPage);
				mPage->fixCount = 1;
				mPage->pHandle->pageNum = pageNum;
				free(mPage->pHandle->data);
				mPage->pHandle->data = ph;
				return RC_OK;
		} else return RC_BM_ALL_PAGES_PINNED;
}



// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	int i;
	PageNumber *contents = malloc(bm->numPages * sizeof(PageNumber));
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
	mList = (BM_MetaList *)bm->mgmtData;
	i=0;
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		contents[i]=mPage->pHandle->pageNum ;
		i++;
	}
	
	return contents;
}


bool *getDirtyFlags (BM_BufferPool *const bm)
{
	int i;
	bool *dirty = malloc(bm->numPages*sizeof(bool));
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
	mList = (BM_MetaList *)bm->mgmtData;
	i=0;
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		dirty[i]=mPage->dirtyFlag;
		i++;
	}
	
	return dirty;
}


int *getFixCounts (BM_BufferPool *const bm)
{
	int i;
	int *fix = malloc(bm->numPages*sizeof(int));
	BM_MetaList *mList;
	BM_MetaPage *mPage;
	linkedListNode *pNode;
	mList = (BM_MetaList *)bm->mgmtData;
	i=0;
	for(pNode = mList->pList->head; pNode != NULL; pNode= pNode->next) {
		mPage = (BM_MetaPage *) pNode->value ; 
		fix[i]=mPage->fixCount;
		i++;
	}
	
	return fix;
}


int getNumReadIO (BM_BufferPool *const bm)
{
	BM_MetaList *mList;
	mList = (BM_MetaList *)bm->mgmtData;
	return mList->numReadIO;
}


int getNumWriteIO (BM_BufferPool *const bm)
{
	BM_MetaList *mList;
	mList = (BM_MetaList *)bm->mgmtData;
	return mList->numWriteIO;
}




