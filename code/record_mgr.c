#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "record_mgr.h"

// table and manager
extern RC initRecordManager (void *mgmtData)
{
    return RC_OK ; 
}

extern RC shutdownRecordManager ()
{
    return RC_OK ; 
}

extern RC createTable (char *name, Schema *schema)
{
	char *tabInfo, *schemaStr;
	char *emptyDataPage;
	VarString *result;
	MAKE_VARSTRING(result);
	APPEND(result, "TABLE <%s> with <%i> tuples in <%i> pages:\n", name, 0,1);
	APPEND(result, "First Free Page Num  <%i> page:\n", 1);
	schemaStr=serializeSchema(schema);
	APPEND_STRING(result, schemaStr);
	free(schemaStr);
  	GET_STRING(tabInfo,result);  
	FREE_VARSTRING(result);

	createPageFile(name);
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	initBufferPool(bm, name,RM_BUFFER_SIZE, RS_FIFO, NULL);
	pinPage(bm,h,0);
	sprintf(h->data, "%s", tabInfo);
	markDirty(bm,h);
	unpinPage(bm,h);

	emptyDataPage=createEmptyPage(schema);
	pinPage(bm,h,1);
	sprintf(h->data, "%s", emptyDataPage);
	markDirty(bm,h);
	unpinPage(bm,h);
	free(h);
	free(tabInfo);
	free(emptyDataPage);	
	shutdownBufferPool(bm);
	free(bm);
    return RC_OK ; 
}

extern RC openTable (RM_TableData *rel, char *name)
{
	int numAttr,numTuples,numPages,firstFree;
	char *schemaPage= (char*) malloc(PAGE_SIZE);
	char *tmp;
	SM_FileHandle fh;
	BM_PageHandle *h;
	rel->name=name;
	h = MAKE_PAGE_HANDLE();
	RM_RelData *rData = (RM_RelData *)malloc(sizeof(RM_RelData));
	rData->bm=MAKE_POOL();
	initBufferPool(rData->bm,name,RM_BUFFER_SIZE,RS_FIFO,NULL);
	openPageFile (rData->bm->pageFile, &fh);
	rData->numDataPages = fh.totalNumPages - 1;
	closePageFile(&fh);
	pinPage(rData->bm,h,0);
	strncpy(schemaPage,h->data,PAGE_SIZE);
	unpinPage(rData->bm,h);
	rel->schema = stringToTabInfo(schemaPage,&tmp,&numTuples,&numPages,&firstFree);
	rData->totalNumberOfRecords = numTuples;
	rData->numDataPages = numPages;
	rData->firstFreePage=firstFree;
	rel->mgmtData = rData;
	free(tmp);
	free(schemaPage);
	rel->mgmtData=rData;
    return RC_OK ; 
}

extern RC closeTable (RM_TableData *rel)
{
	RM_RelData *rData = (RM_RelData*) rel->mgmtData;
	if(shutdownBufferPool(rData->bm) == RC_BM_POOL_HAS_PINNED_PAGES) {
		return RC_RM_CANNOT_CLOSE_TABLE_IN_USE;
	}
	free(rData->bm);
	free(rData);
	freeSchema(rel->schema);
    return RC_OK ; 
}

extern RC deleteTable (char *name)
{
	destroyPageFile(name);
    return RC_OK ; 
}

extern int getNumTuples (RM_TableData *rel)
{
	RM_RelData *rData = (RM_RelData *)rel->mgmtData;
    return rData->totalNumberOfRecords; 
}

extern int getNumPages (RM_TableData *rel)
{
	RM_RelData *rData = (RM_RelData *)rel->mgmtData;
    return rData->numDataPages; 
}

extern int getFirstFreePage(RM_TableData *rel)
{
	RM_RelData *rData = (RM_RelData *)rel->mgmtData;
    return rData->firstFreePage; 
}

extern char * createEmptyPage(Schema *schema) 
{
	VarString *result;
	int i,recordsPerPage;
	recordsPerPage= getNumRecordsPerPage(schema);
	MAKE_VARSTRING(result);
	for(i = 0; i < recordsPerPage; i++)
		APPEND(result, "%s%i", (i != 0) ? "," : "", 0);
	APPEND_STRING(result,"\n");
	APPEND_STRING(result,"Next Free Page <-1>\n");
	for(i = 0; i < recordsPerPage; i++)
		APPEND_STRING(result," \n");
	RETURN_STRING(result);
}



// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	int i,numFreeSlots,firstFreePage,nextFreePage,firstFreeSlot;
	int recordsPerPage,splitCnt1,splitCnt2;
	int *recEmptyFlag;
	char **pageComp, **flagComp, **nextFreeComp;
	char *dataPage, *delim, *updatedPage, *emptyPage, *recordStr, *tabInfo;
	VarString *updatedStr;
	BM_PageHandle *h, *h0, *hnext;
	MAKE_VARSTRING(updatedStr);
	dataPage = (char*) malloc(PAGE_SIZE);
	RM_RelData *rData = (RM_RelData*) rel->mgmtData;
	firstFreePage = rData->firstFreePage;
	h = MAKE_PAGE_HANDLE();
	pinPage(rData->bm,h,firstFreePage);
	strncpy(dataPage,h->data,PAGE_SIZE);

	delim="\n";
	pageComp = strsplit(dataPage,delim,&splitCnt1);
	delim=",";
	flagComp = strsplit(pageComp[0],delim,&recordsPerPage);
	delim = "<>";
	nextFreeComp = strsplit(pageComp[1],delim,&splitCnt2);
	nextFreePage = atoi(nextFreeComp[1]);
	freeStrArr(nextFreeComp,splitCnt2);
	free(nextFreeComp);

	recEmptyFlag = (int *) malloc(sizeof(int)*recordsPerPage);
	numFreeSlots=0;
	firstFreeSlot =-1;
	for(i =0; i<recordsPerPage; i++) {
		recEmptyFlag[i]=atoi(flagComp[i]);
		if(atoi(flagComp[i])==0) {
			numFreeSlots= numFreeSlots+1;
			if(firstFreeSlot==-1) firstFreeSlot= i;
		}
	}
	freeStrArr(flagComp,recordsPerPage);
	free(flagComp);
	record->id.page = firstFreePage;
	record->id.slot = firstFreeSlot;
	recordStr = serializeRecord(record,rel->schema);
	recEmptyFlag[firstFreeSlot] = 1;
	free(pageComp[firstFreeSlot+2]);
	pageComp[firstFreeSlot+2]=strdup(recordStr);
	rData->totalNumberOfRecords=rData->totalNumberOfRecords+1;
	for(i = 0;i < recordsPerPage;i++) 
		APPEND(updatedStr, "%s%i", (i != 0) ? "," : "", recEmptyFlag[i]);
	APPEND_STRING(updatedStr,"\n");
//	printf("Num Free Slots <%i>\n", numFreeSlots);
	if(numFreeSlots==1) {
		//manage nextFreePage
		if(nextFreePage=-1) {
			//create new emptyPage
			emptyPage = createEmptyPage(rel->schema);
			rData->numDataPages = rData->numDataPages +1;
			hnext = MAKE_PAGE_HANDLE();
			pinPage(rData->bm,hnext,rData->numDataPages);
			sprintf(hnext->data,"%s",emptyPage);
			markDirty(rData->bm, hnext);
			unpinPage(rData->bm, hnext);
			//update nextFreePage
			nextFreePage=rData->numDataPages;
			free(emptyPage);
			free(hnext);
		}
		//update page 0 tableInfo
		rData->firstFreePage = nextFreePage;
	}

	h0 = MAKE_PAGE_HANDLE();
	pinPage(rData->bm,h0,0);
	tabInfo = serializeTableInfo(rel);
	sprintf(h0->data,"%s",tabInfo);
	free(tabInfo);
	markDirty(rData->bm,h0);
	unpinPage(rData->bm,h0);
	APPEND(updatedStr,"Next Free Page <%i>\n",nextFreePage);
	for(i = 0; i< recordsPerPage; i++) {
		APPEND(updatedStr,"%s\n",pageComp[i+2]);
	}
//printf("Test\n");
	GET_STRING(updatedPage,updatedStr);
	sprintf(h->data,"%s",updatedPage);
	markDirty(rData->bm,h);
	unpinPage(rData->bm,h);

	freeStrArr(pageComp,splitCnt1);
	free(pageComp);
	free(dataPage);
	free(updatedPage);
	free(recordStr);
	free(recEmptyFlag);
	free(h);
	free(h0);
    return RC_OK ; 
}

extern RC deleteRecord (RM_TableData *rel, RID id)
{
	char *dataPage, *delim, *updatedPage,*tabInfo;
	int *recEmptyFlag;
	int i,splitCnt1,splitCnt2, recordsPerPage,numFreeSlots, nextFreePage;
	char **pageComp,**nextFreeComp, **flagComp;
	VarString *updatedStr;
	BM_PageHandle *h, *h0;
	RM_RelData *rData=(RM_RelData*)rel->mgmtData;
	MAKE_VARSTRING(updatedStr);

	dataPage = (char*) malloc(PAGE_SIZE);
	h=MAKE_PAGE_HANDLE();

	pinPage(rData->bm,h,id.page);
	strncpy(dataPage,h->data,PAGE_SIZE);
	delim="\n";
	pageComp = strsplit(dataPage,delim,&splitCnt1);
	delim=",";
	flagComp = strsplit(pageComp[0],delim,&recordsPerPage);
	recEmptyFlag = (int *) malloc(sizeof(int)*recordsPerPage);
	numFreeSlots=0;
	for(i =0; i<recordsPerPage; i++) {
		recEmptyFlag[i]=atoi(flagComp[i]);
		if(atoi(flagComp[i])==0) numFreeSlots=numFreeSlots+1;
	}
	freeStrArr(flagComp,recordsPerPage);
	free(flagComp);

	delim = "<>";
	nextFreeComp = strsplit(pageComp[1],delim,&splitCnt2);
	nextFreePage = atoi(nextFreeComp[1]);
	freeStrArr(nextFreeComp,splitCnt2);
	free(nextFreeComp);

	if(recEmptyFlag[id.slot]==0) return RC_RM_RECORD_NOT_FOUND;
	recEmptyFlag[id.slot] = 0;
	free(pageComp[2+id.slot]);
	pageComp[2+id.slot]=strdup(" ");
	rData->totalNumberOfRecords = rData->totalNumberOfRecords - 1;
	if(numFreeSlots==0) {
		//Manage free Space
		nextFreePage = rData->firstFreePage;
		rData->firstFreePage=id.page;
	}
	h0 = MAKE_PAGE_HANDLE();
	pinPage(rData->bm,h0,0);
	tabInfo = serializeTableInfo(rel);
	sprintf(h0->data,"%s",tabInfo);
	free(tabInfo);
	markDirty(rData->bm,h0);
	unpinPage(rData->bm,h0);
	
	for(i = 0;i < recordsPerPage;i++) 
		APPEND(updatedStr, "%s%i", (i != 0) ? "," : "", recEmptyFlag[i]);
	APPEND_STRING(updatedStr,"\n");
	APPEND(updatedStr,"Next Free Page <%i>\n",nextFreePage);
	for(i = 0; i< recordsPerPage; i++) {
		APPEND(updatedStr,"%s\n",pageComp[i+2]);
	}
	GET_STRING(updatedPage,updatedStr);
	sprintf(h->data,"%s",updatedPage);
	markDirty(rData->bm,h);
	unpinPage(rData->bm,h);
	freeStrArr(pageComp,splitCnt1);
	free(pageComp);
	free(dataPage);
	free(updatedPage);
	free(recEmptyFlag);	
	free(h);
	free(h0);
    return RC_OK ; 
}

extern RC updateRecord (RM_TableData *rel, Record *record)
{
	char *dataPage, *delim, *updatedPage, *recordStr;
	int *recEmptyFlag;
	int i,splitCnt1,splitCnt2, recordsPerPage,numFreeSlots, nextFreePage;
	char **pageComp,**nextFreeComp, **flagComp;
	VarString *updatedStr;
	BM_PageHandle *h, *h0;
	RM_RelData *rData=(RM_RelData*)rel->mgmtData;
	MAKE_VARSTRING(updatedStr);

	dataPage = (char*) malloc(PAGE_SIZE);
	h=MAKE_PAGE_HANDLE();

	pinPage(rData->bm,h,record->id.page);
	strncpy(dataPage,h->data,PAGE_SIZE);
	delim="\n";
	pageComp = strsplit(dataPage,delim,&splitCnt1);
	delim=",";
	flagComp = strsplit(pageComp[0],delim,&recordsPerPage);
	recEmptyFlag = (int *) malloc(sizeof(int)*recordsPerPage);
	numFreeSlots=0;
	for(i =0; i<recordsPerPage; i++) {
		recEmptyFlag[i]=atoi(flagComp[i]);
		if(atoi(flagComp[i])==0) numFreeSlots=numFreeSlots+1;
	}
	freeStrArr(flagComp,recordsPerPage);
	free(flagComp);

	delim = "<>";
	nextFreeComp = strsplit(pageComp[1],delim,&splitCnt2);
	nextFreePage = atoi(nextFreeComp[1]);
	freeStrArr(nextFreeComp,splitCnt2);
	free(nextFreeComp);

	if(recEmptyFlag[record->id.slot]==0) return RC_RM_RECORD_NOT_FOUND;
	recordStr = serializeRecord(record,rel->schema);
	free(pageComp[2+record->id.slot]);
	pageComp[2+record->id.slot]=strdup(recordStr);
	
	for(i = 0;i < recordsPerPage;i++) 
		APPEND(updatedStr, "%s%i", (i != 0) ? "," : "", recEmptyFlag[i]);
	APPEND_STRING(updatedStr,"\n");
	APPEND(updatedStr,"Next Free Page <%i>\n",nextFreePage);
	for(i = 0; i< recordsPerPage; i++) {
		APPEND(updatedStr,"%s\n",pageComp[i+2]);
	}
	GET_STRING(updatedPage,updatedStr);
	sprintf(h->data,"%s",updatedPage);
	markDirty(rData->bm,h);
	unpinPage(rData->bm,h);

	freeStrArr(pageComp,splitCnt1);
	free(pageComp);
	free(dataPage);
	free(updatedPage);
	free(recEmptyFlag);	
	free(recordStr);
	free(h);
    return RC_OK ; 
}

extern RC getRecord (RM_TableData *rel, RID id, Record **record)
{
	char *dataPage, *delim;
	int *recEmptyFlag;
	int i,splitCnt1, recordsPerPage,numFreeSlots;
	char **pageComp, **flagComp;
	BM_PageHandle *h;
	RM_RelData *rData=(RM_RelData*)rel->mgmtData;

	dataPage = (char*) malloc(PAGE_SIZE);
	h=MAKE_PAGE_HANDLE();

	pinPage(rData->bm,h,id.page);
	strncpy(dataPage,h->data,PAGE_SIZE);
	unpinPage(rData->bm,h);
	delim="\n";
	pageComp = strsplit(dataPage,delim,&splitCnt1);
	delim=",";
	flagComp = strsplit(pageComp[0],delim,&recordsPerPage);
	recEmptyFlag = (int *) malloc(sizeof(int)*recordsPerPage);
	numFreeSlots=0;
	for(i =0; i<recordsPerPage; i++) {
		recEmptyFlag[i]=atoi(flagComp[i]);
		if(atoi(flagComp[i])==0) numFreeSlots=numFreeSlots+1;
	}
	freeStrArr(flagComp,recordsPerPage);
	free(flagComp);


	if(recEmptyFlag[id.slot]==0) return RC_RM_RECORD_NOT_FOUND;
	else (*record) = stringToRecord(pageComp[2+id.slot],rel->schema);
	freeStrArr(pageComp,splitCnt1);
	free(pageComp);
	free(dataPage);
	free(recEmptyFlag);	
	free(h);
 
    return RC_OK ; 
}


// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	RM_MetaScan *mScan = (RM_MetaScan*) malloc(sizeof(RM_MetaScan));
	scan->rel = rel;
	mScan->iterator.page=1;
	mScan->iterator.slot=-1;
	mScan->cond=cond;
	scan->mgmtData = mScan;
    return RC_OK ; 
}

extern RC next (RM_ScanHandle *scan, Record **record)
{
	RM_MetaScan *mScan = (RM_MetaScan*) scan->mgmtData;
	RM_TableData *rel = (RM_TableData*)scan->rel;
	RM_RelData *rData = (RM_RelData*) rel->mgmtData;
	Record *r ;
	Value *match;
	int totalNumberOfPage, recordsPerPage;
	int conditionNotSatisfied=1;
	int hasMoreRecords=1;
	totalNumberOfPage = rData->numDataPages;
	recordsPerPage= getNumRecordsPerPage(rel->schema);

	while(conditionNotSatisfied & hasMoreRecords) {
		//increment rid
		if(mScan->iterator.slot<(recordsPerPage-1)) {
			mScan->iterator.slot = mScan->iterator.slot+1;
		} else if(mScan->iterator.page<(totalNumberOfPage-1)) {
			mScan->iterator.page=mScan->iterator.page+1;
			mScan->iterator.slot=0;
		} else {
			hasMoreRecords=0;
			break;
		}
		//getRecord
		if(getRecord(rel,mScan->iterator,&r)==RC_OK) {
			//check if condition satisfied
			if(mScan->cond == NULL) {
				conditionNotSatisfied=0;
			} else {
				evalExpr(r,rel->schema,mScan->cond,&match);
				if(match->v.boolV) 
					conditionNotSatisfied=0;
			}
		}
	}

	if(!hasMoreRecords) return RC_RM_NO_MORE_TUPLES;
	if(!conditionNotSatisfied) (*record) = r;
    return RC_OK ; 
}

extern RC closeScan (RM_ScanHandle *scan)
{
	
	RM_MetaScan *mScan = (RM_MetaScan*) scan->mgmtData;
	free(mScan);
    return RC_OK ; 
}


// dealing with schemas
extern int getRecordSize (Schema *schema)
{
	int i;
	int recordSize=0;
	for (i = 0; i< schema->numAttr ; i++) 
		switch (schema->dataTypes[i])
      	{
			case DT_STRING:
				recordSize += schema->typeLength[i];
				break;
			case DT_INT:
				recordSize += sizeof(int);
				break;
			case DT_FLOAT:
				recordSize += sizeof(float);
				break;
			case DT_BOOL:
				recordSize += sizeof(bool);
				break;
  		}
 
    return recordSize; 
}
extern int getSerializedRecordSize (Schema *schema) {
	//[1-0] (a:0,b:aaaa,c:3)
	int i;
	int recordSize=(2*SERIALIZED_INT) +6;
	for (i = 0; i< schema->numAttr ; i++) 
	{
		recordSize = recordSize+strlen(schema->attrNames[i])+2;
		switch (schema->dataTypes[i])
      		{
			case DT_STRING:
				recordSize += schema->typeLength[i];
				break;
			case DT_INT:
				recordSize += SERIALIZED_INT;
				break;
			case DT_FLOAT:
				recordSize += SERIALIZED_INT;
				break;
			case DT_BOOL:
				recordSize += SERIALIZED_INT;
				break;
  		}
	}
 
    return recordSize; 

}

extern int getNumRecordsPerPage (Schema *schema) 
{	
	return floor((PAGE_SIZE - TABLE_META_SIZE)/getSerializedRecordSize(schema));
}

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *s;
	s = (Schema *) malloc(sizeof(Schema)); 
	s->numAttr = numAttr;
	s->attrNames= attrNames;
	s->dataTypes = dataTypes;
	s->typeLength = typeLength;
	s->keySize = keySize;
	s->keyAttrs = keys;
    return s; 
}

extern RC freeSchema (Schema *schema)
{
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	free(schema);
    return RC_OK ; 
}


// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema)
{
	int size=getRecordSize(schema);
	(*record) = (Record *) malloc(sizeof(Record));
	(*record)->data = (char*) malloc(size);
    return RC_OK ; 
}

extern RC freeRecord (Record *record)
{
	free(record->data);
	free(record);
    return RC_OK ; 
}

extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset;
	char *attrData;

	attrOffset(schema,attrNum, &offset);
	attrData = record->data + offset;


	(*value) = (Value *) malloc(sizeof(Value));
	switch (schema->dataTypes[attrNum])
  	{
		case DT_STRING:
			{
				char *buf;
				int len = schema->typeLength[attrNum];
				buf = (char *) malloc(len + 1);
				strncpy(buf, attrData, len);
				buf[len] = '\0';
				(*value)->dt=DT_STRING;
				(*value)->v.stringV=buf;
			}
			break;
		case DT_INT:
			{
				int val = 0;
				memcpy(&val,attrData, sizeof(int));
				(*value)->dt=DT_INT;
				(*value)->v.intV=val;
			}
			break;
		case DT_FLOAT:
			{
				float val;
				memcpy(&val,attrData, sizeof(float));
				(*value)->dt=DT_FLOAT;
				(*value)->v.floatV=val;
			}
			break;
		case DT_BOOL:
			{
				bool val;
				memcpy(&val,attrData, sizeof(bool));
				(*value)->dt=DT_BOOL;
				(*value)->v.boolV=val;
			}
			break;
	}
 

    return RC_OK ; 
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset;
	char *attrData;

	attrOffset(schema,attrNum, &offset);
	attrData = record->data + offset;
	
	if(schema->dataTypes[attrNum] == value->dt) {
		switch (schema->dataTypes[attrNum])
		{
			case DT_STRING:
				{
					int len = schema->typeLength[attrNum];
					strncpy(attrData,value->v.stringV, len);
				}
				break;
			case DT_INT:
				{
					memcpy(attrData,&(value->v.intV), sizeof(int));
				}
				break;
			case DT_FLOAT:
				{
					memcpy(attrData,&(value->v.floatV), sizeof(float));
				}
				break;
			case DT_BOOL:
				{
					memcpy(attrData,&(value->v.boolV), sizeof(bool));
				}
				break;
		}
	} else {
		return RC_RM_ASSIGN_VALUE_OF_DIFFERENT_DATATYPE; 
	}

    return RC_OK ; 

}


