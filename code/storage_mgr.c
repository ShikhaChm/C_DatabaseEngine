#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

void initStorageManager (void) {
	printf("Initializing Storage Manager v 1.0.0!\n");
}
RC createPageFile (char *fileName)
{
	FILE *fptr;
	int sizeCnt=0;
	fptr = fopen(fileName, "rb");

	if(fptr== NULL) //if file does not exist, create it
	{
		fptr=fopen(fileName, "w+");
		if(fptr)
		{
			// Fill the newly created file  with '\0'.
			//Create Meta Data 
			fprintf(fptr,"%i", 1);
			fseek(fptr, ((int)META_SIZE), SEEK_SET);	
			//Create 1st page filled with nulls
			for(sizeCnt=META_SIZE; sizeCnt < (int )(PAGE_SIZE+META_SIZE); sizeCnt+=sizeof('\0')) {
				putc('\0',fptr);
			}
			fclose(fptr);
		}
		return RC_OK;

	}
	else
		return RC_FILE_ALREADY_EXISTS ;
}

RC openPageFile(char *fileName,SM_FileHandle *fHandle)
{
	FILE *fptr;
	int sizeCnt=0;
	fptr = fopen(fileName, "r+wb");

	if(fptr== NULL) //if file does not exist, create it
		return RC_FILE_NOT_FOUND;
	else { 
		fHandle->fileName=fileName;
		int totalPages;
		fseek(fptr, 0, SEEK_SET);	
		fscanf(fptr,"%d", &totalPages);
		fHandle->totalNumPages= totalPages;
		fseek(fptr, ((int)META_SIZE), SEEK_SET);	
		fHandle->curPagePos= 0;
		fHandle->mgmtInfo=fptr;
		return RC_OK;
	}
}

RC closePageFile(SM_FileHandle *fHandle)
{
	FILE *fptr= (FILE *)fHandle->mgmtInfo;
	if( fptr==NULL) {
		return RC_FILE_HANDLE_NOT_INIT;
	} else {
		fclose(fptr);
		return RC_OK;
	}
}
RC destroyPageFile(char *fileName)
{
	remove(fileName);
	return RC_OK;
}

/* Reading blocks from disc */
RC readBlock (int pageNum,SM_FileHandle *fHandle,SM_PageHandle memPage)
{
	int startPosition,i;
	FILE * fptr;

	if((pageNum < 0) || (pageNum > (fHandle->totalNumPages - 1) )) {
		return RC_READ_NON_EXISTING_PAGE;
	} else {
		startPosition = ((int) META_SIZE) + (pageNum*PAGE_SIZE);
		fptr=((FILE *)fHandle->mgmtInfo);
		fseek(fptr, startPosition, SEEK_SET);
		fread(memPage, 1,PAGE_SIZE,fptr);
		fHandle->curPagePos = pageNum;
		return RC_OK;
	}
}

int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle,SM_PageHandle memPage)
{
	readBlock(0,fHandle,memPage);
	return RC_OK;
}

RC readPreviousBlock(SM_FileHandle *fHandle,SM_PageHandle memPage)
{
	readBlock((fHandle->curPagePos - 1),fHandle,memPage);
	return RC_OK;

}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	readBlock(fHandle->curPagePos ,fHandle,memPage);
	return RC_OK;

}

RC readNextBlock(SM_FileHandle *fHandle,SM_PageHandle memPage)
{
	readBlock((fHandle->curPagePos + 1),fHandle,memPage);
	return RC_OK;

}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	readBlock((fHandle->totalNumPages - 1),fHandle,memPage);
	return RC_OK;

}

RC writeBlock (int pageNum,SM_FileHandle *fHandle,SM_PageHandle memPage) {
	int startPosition,i;
	FILE * fptr;
	//SM_PageHandle ph;
	//ph = (SM_PageHandle) malloc(PAGE_SIZE);
	if(pageNum >= fHandle->totalNumPages) ensureCapacity((pageNum+1),fHandle);
	startPosition = ((int) META_SIZE) + (pageNum*PAGE_SIZE);
	fptr=((FILE *)fHandle->mgmtInfo);
	
	fseek(fptr, startPosition, SEEK_SET);
	fwrite(memPage,PAGE_SIZE,1,fptr);
	//fwrite(memPage,1,PAGE_SIZE,fptr);
	//fseek(fptr, startPosition, SEEK_SET);
	//fread(ph, PAGE_SIZE,1,fptr);

	fHandle->curPagePos= pageNum;
	return RC_OK;
}
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	writeBlock(fHandle->curPagePos,fHandle,memPage);
	return RC_OK;
}
RC appendEmptyBlock(SM_FileHandle *fHandle) {
	int i;
	FILE *fptr;
	fptr = (FILE *) fHandle->mgmtInfo;
	fseek(fptr,0, SEEK_END);
        //Create 1st page filled with nulls
        for(i=0; i < ((int )PAGE_SIZE)/sizeof('\0'); i++) {
                putc('\0',fptr);
        }
	fHandle->totalNumPages +=1;
	fseek(fptr, 0, SEEK_SET);	
	fprintf(fptr,"%i", fHandle->totalNumPages);
	fseek(fptr,0, SEEK_END);
	return RC_OK;
}
RC ensureCapacity(int numberOfPages,SM_FileHandle *fHandle) {
	int i;
	if(fHandle->totalNumPages < numberOfPages) {
		for(i=0;i < (numberOfPages - fHandle->totalNumPages); i++) appendEmptyBlock(fHandle);
	}
	return RC_OK;
}

