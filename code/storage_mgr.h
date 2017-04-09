#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/*------------------------- manipulating page files-------------------------------- */

 /*Just prints a message */
extern void initStorageManager (void);    

/* Function:   creates a single page file. Fills it with '\0'
   Parameters: CHAR *fileName
   Returns:     RC_OK if the new file is created else returns RC_FILE_ALREADY_EXISTS  */
extern RC createPageFile (char *fileName);

/* Function:   opens a file. 
   Parameters: *fileName,*fHandle
   Returns:    if file pointer== NULL returns RC_FILE_NOT_FOUND else initializes values of SM_FileHandle,sets curPagePos to 0,opens the file and returns RC_OK  */
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);

/* Function:   closes a file. 
   Parameters: *fHandle
   Returns:    if file pointer== NULL returns RC_FILE_HANDLE_NOT_INIT else closes file and returns RC_OK */
extern RC closePageFile (SM_FileHandle *fHandle);

/* Function:   Deletes a file and it is no longer accessible. 
   Parameters: *fileName
   Returns:    deletes file and returns RC_OK */
extern RC destroyPageFile (char *fileName);

/* -----------------------reading blocks from disc---------------------- */

/* Function:   Reads the required page. 
   Parameters: pageNum,*fHandle,SM_PageHandle memPage
   Returns:    RC_READ_NON_EXISTING_PAGE if page before first and page after last is read in  file. Else sets position at start of first page,reads one page,increment curPagePos by 1 and returns RC_OK */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function:   Returns the current page number. 
   Parameters: *fHandle
   Returns:   current page number */
extern int getBlockPos (SM_FileHandle *fHandle);

/* Function:   Reads the first page. It calls readBlock function with pageNum=0(first page).   Parameters: *fHandle, memPage
   Returns:    RC_OK */
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function:  Reads the page previous to the current page number calling readBlock function.
   Parameters:*fHandle,SM_PageHandle memPage
   Returns:   RC_OK */
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function:   Calls readBlock function to read the page number as per the value of curPagePos. 
   Parameters: *fHandle,SM_PageHandle memPage
   Returns:    RC_READ_NON_EXISTING_PAGE if page before first and page after last is read in  file. Else sets position at start of first page,reads one page,increment curPagePos by 1 and returns RC_OK */
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function: Calls readBlock function to read the next page at number curPagePos+1. 
   Parameters: *fHandle,SM_PageHandle memPage
   Returns:    RC_OK */
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function:   Calls readBlock and reads last page which has number= totalNumPages-1. 
   Parameters: *fHandle,SM_PageHandle memPage
   Returns:    RC_OK */
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/*-----------------------writing blocks to a page file------------------------*/

/* Function:   Writes on the file page,sets the poisition to the beginning of the written page then reads the page. Also increments the current page position by 1.
   Parameters: pageNum,*fHandle,SM_PageHandle memPage
   Returns:    RC_OK */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function:   Writes on page number given by current page position.
   Parameters: *fHandle,SM_PageHandle memPage
   Returns:    RC_OK */
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* Function:   Adds a new page in the file. Fills the new last page with zero bytes.
   Parameters: *fHandle
   Returns:    RC_OK */
extern RC appendEmptyBlock (SM_FileHandle *fHandle);

/* Function:  Checks if the file has less than numberOfPages pages,if yes then increase the size to numberOfPages.
   Parameters: numberOfPages,*fHandle
   Returns:    RC_OK */
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);

#endif
