#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"

// prototypes

// implementations
char *
serializeTableInfo(RM_TableData *rel)
{
  char *schemaStr;
  VarString *result;
  MAKE_VARSTRING(result);
  APPEND(result, "TABLE <%s> with <%i> tuples in <%i> pages:\n", rel->name, getNumTuples(rel),getNumPages(rel));
  APPEND(result, "First Free Page Num  <%i> page:\n", getFirstFreePage(rel));
  schemaStr=serializeSchema(rel->schema);
  APPEND_STRING(result, schemaStr);
  free(schemaStr);
  RETURN_STRING(result);  
}

Schema *stringToTabInfo(char *tabInfo, char **name,int *numTuples, int *numPages, int *firstFreePage) 
{
	Schema *result;
	char **infoComp, **nameNum;
	char *delim;
	int i,splitCnt1, splitCnt2;
	delim = "\n";
	infoComp = strsplit(tabInfo,delim, &splitCnt1);
	delim = "<>";
	nameNum = strsplit(infoComp[0],delim,&splitCnt2);
	(*name) = strdup(nameNum[1]);
	(*numTuples) = atoi(nameNum[3]);
	(*numPages)  = atoi(nameNum[5]);
	for(i=0;i<splitCnt2;i++) free(nameNum[i]);
	free(nameNum);
	nameNum = strsplit(infoComp[1],delim,&splitCnt2);
	(*firstFreePage) = atoi(nameNum[1]);
	result = stringToSchema(infoComp[2]);

	freeStrArr(infoComp,splitCnt1);
	free(infoComp);
	freeStrArr(nameNum,splitCnt2);
	free(nameNum);
	return result;
}

char * 
serializeTableContent(RM_TableData *rel)
{
  int i;
  char *recordStr;
  VarString *result;
  RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
  Record *r = (Record *) malloc(sizeof(Record));
  MAKE_VARSTRING(result);

  for(i = 0; i < rel->schema->numAttr; i++)
    APPEND(result, "%s%s", (i != 0) ? ", " : "", rel->schema->attrNames[i]);

  startScan(rel, sc, NULL);
  
  while(next(sc, &r) != RC_RM_NO_MORE_TUPLES) 
    {
	recordStr = serializeRecord(r, rel->schema);
    APPEND_STRING(result,recordStr);
	free(recordStr);
    APPEND_STRING(result,"\n");
    }
  closeScan(sc);

  RETURN_STRING(result);
}


char * 
serializeSchema(Schema *schema)
{
  int i;
  VarString *result;
  MAKE_VARSTRING(result);

  APPEND(result, "Schema with <%i> attributes (", schema->numAttr);
  for(i = 0; i < schema->numAttr; i++)
    {
      APPEND(result,"%s%s:", (i != 0) ? ",": "", schema->attrNames[i]);
      switch (schema->dataTypes[i])
	{
	case DT_INT:
	  APPEND_STRING(result, "INT");
	  break;
	case DT_FLOAT:
	  APPEND_STRING(result, "FLOAT");
	  break;
	case DT_STRING:
	  APPEND(result,"STRING[%i]", schema->typeLength[i]);
	  break;
	case DT_BOOL:
	  APPEND_STRING(result,"BOOL");
	  break;
	}
    }
  APPEND_STRING(result,")");

  APPEND_STRING(result," with keys: (");

  for(i = 0; i < schema->keySize; i++)
    APPEND(result, "%s%i", ((i != 0) ? ",": ""), schema->keyAttrs[i]); 

  APPEND_STRING(result,")\n");

  RETURN_STRING(result);
}

Schema *stringToSchema(char *schemaLine) 
{
	Schema *scma = (Schema *) malloc(sizeof(Schema));
	int numAttr,i,j,splitCnt1,splitCnt2,splitCnt3;
	char **components, **tmp, **tmpStr;
	char *delim;
	char *schemaAttr;
	char *schemaKeys;
	delim = "<>()";
	components=strsplit(schemaLine,delim,&splitCnt1);
	numAttr=atoi(components[1]);
	scma->numAttr=numAttr;
	scma->attrNames=(char **) malloc(sizeof(char*)*numAttr);
	scma->dataTypes=(DataType *) malloc(sizeof(DataType)*numAttr);
	scma->typeLength=(int *) malloc(sizeof(int)*numAttr);
	schemaAttr = strdup(components[3]);
	schemaKeys = strdup(components[5]);
 	freeStrArr(components,splitCnt1);
	free(components);	
	delim=",";
	components=strsplit(schemaAttr,delim,&splitCnt1);
	if(numAttr != splitCnt1) {printf("Schema String Bad\n");}
	for(i =0 ; i < numAttr; i++) {
		delim=":";
		tmp=strsplit(components[i],delim,&splitCnt2);
		scma->attrNames[i]= strdup(tmp[0]);
		if(strcmp(tmp[1],"INT")==0) {
			scma->dataTypes[i]= DT_INT;
			scma->typeLength[i]=sizeof(int);
		} else if (strcmp(tmp[1],"FLOAT")==0) {
			scma->dataTypes[i]= DT_FLOAT;
			scma->typeLength[i]=sizeof(float);
		} else if (strcmp(tmp[1],"BOOL")==0) {
			scma->dataTypes[i]= DT_BOOL;
			scma->typeLength[i]=sizeof(bool);
		} else if (strstr(tmp[1],"STRING") != NULL) {
			delim="[]";
			tmpStr=strsplit(tmp[1],delim,&splitCnt3);
			scma->dataTypes[i]= DT_STRING;
			scma->typeLength[i]=atoi(tmpStr[1]);
			freeStrArr(tmpStr,splitCnt3);
			free(tmpStr);
		}
		freeStrArr(tmp,splitCnt2);
		free(tmp);
	}

 	freeStrArr(components,splitCnt1);
	free(components);	
	delim=",";
	components=strsplit(schemaKeys,delim,&splitCnt1);
	scma->keySize=splitCnt1;
	scma->keyAttrs =(int *) malloc(sizeof(int)*splitCnt1);
	for(i=0; i<splitCnt1; i++) {
		scma->keyAttrs[i]=atoi(components[i]);
	}

 	freeStrArr(components,splitCnt1);
	free(components);
	free(schemaAttr);
	free(schemaKeys);
	return scma;	
}

char * 
serializeRecord(Record *record, Schema *schema)
{
  char *attrStr;
  VarString *result;
  MAKE_VARSTRING(result);
  int i;
  
  APPEND(result, "[%i-%i] (", record->id.page, record->id.slot);
  for(i = 0; i < schema->numAttr; i++)
    {
      APPEND(result, "%s", (i == 0) ? "" : ",");
      attrStr= serializeAttr (record, schema, i);
      APPEND_STRING(result, attrStr);
      free(attrStr);
    }
  
  APPEND_STRING(result, ")");

  RETURN_STRING(result);
}

char * 
serializeAttr(Record *record, Schema *schema, int attrNum)
{
  int offset;
  char *attrData;
  VarString *result;
  MAKE_VARSTRING(result);
  
  attrOffset(schema, attrNum, &offset);
  attrData = record->data + offset;
  switch(schema->dataTypes[attrNum])
    {
    case DT_INT:
      {
	int val = 0;
	memcpy(&val,attrData, sizeof(int));
	APPEND(result, "%s:%i", schema->attrNames[attrNum], val);
      }
      break;
    case DT_STRING:
      {
	char *buf;
	int len = schema->typeLength[attrNum];
	buf = (char *) malloc(len + 1);
	strncpy(buf, attrData, len);
	buf[len] = '\0';
	
	APPEND(result, "%s:%s", schema->attrNames[attrNum], buf);
	free(buf);
      }
      break;
    case DT_FLOAT:
      {
	float val;
	memcpy(&val,attrData, sizeof(float));
	APPEND(result, "%s:%f", schema->attrNames[attrNum], val);
      }
      break;
    case DT_BOOL:
      {
	bool val;
	memcpy(&val,attrData, sizeof(bool));
	APPEND(result, "%s:%s", schema->attrNames[attrNum], val ? "TRUE" : "FALSE");
      }
      break;
    default:
      return "NO SERIALIZER FOR DATATYPE";
    }

  RETURN_STRING(result);
}

extern Record *stringToRecord(char *recordStr, Schema *schema) 
{
	Record *record ;
	createRecord(&record, schema);
	char **recordComp, **idComp, **attrComp, **eachAttrComp;
	char *delim, *attrData;
	int i, offset, cnt1,cnt2,cnt3,cnt4;
	delim = "[]()";
	recordComp = strsplit(recordStr,delim,&cnt1);
	delim="-";
	idComp = strsplit(recordComp[0],delim,&cnt2);
	record->id.page = atoi(idComp[0]);
	record->id.slot = atoi(idComp[1]);
	freeStrArr(idComp,cnt2);
	free(idComp);
	delim=",";
	attrComp = strsplit(recordComp[2], delim,&cnt3);
	delim =":";
	for(i=0; i < schema->numAttr; i++) {
		eachAttrComp = strsplit(attrComp[i],delim,&cnt4);
		attrOffset(schema, i, &offset);
		attrData=record->data+offset;
		switch (schema->dataTypes[i])
      		{
			case DT_STRING:
			   {
				int len = schema->typeLength[i];
				strncpy(attrData,eachAttrComp[1], len);
				attrData[len] = '\0';
			   }
			   break;
			case DT_INT:
			   {
				int val;
				val = atoi(eachAttrComp[1]);
				memcpy(attrData,&val, sizeof(int));
			   }
			   break;
			case DT_FLOAT:
			   {
				float val;
				val = atof(eachAttrComp[1]);
				memcpy(attrData,&val, sizeof(float));
			   }
			   break;
			case DT_BOOL:
			   {
				bool val;
				if(strcmp(eachAttrComp[1],"TRUE")==0) val=TRUE;
				else val=FALSE;
				memcpy(attrData,&val, sizeof(bool));
			   }
			   break;
  		}

		freeStrArr(eachAttrComp,cnt4);
		free(eachAttrComp);
	}
	freeStrArr(attrComp,cnt3);
	free(attrComp);
	freeStrArr(recordComp,cnt1);
	free(recordComp);
	return record;
}

char *
serializeValue(Value *val)
{
  VarString *result;
  MAKE_VARSTRING(result);
  
  switch(val->dt)
    {
    case DT_INT:
      APPEND(result,"%i",val->v.intV);
      break;
    case DT_FLOAT:
      APPEND(result,"%f", val->v.floatV);
      break;
    case DT_STRING:
      APPEND(result,"%s", val->v.stringV);
      break;
    case DT_BOOL:
      APPEND_STRING(result, ((val->v.boolV) ? "true" : "false"));
      break;
    }

  RETURN_STRING(result);
}

Value *
stringToValue(char *val)
{
  Value *result = (Value *) malloc(sizeof(Value));
  
  switch(val[0])
    {
    case 'i':
      result->dt = DT_INT;
      result->v.intV = atoi(val + 1);
      break;
    case 'f':
      result->dt = DT_FLOAT;
      result->v.floatV = atof(val + 1);
      break;
    case 's':
      result->dt = DT_STRING;
      result->v.stringV = malloc(strlen(val));
      strcpy(result->v.stringV, val + 1);
      break;
    case 'b':
      result->dt = DT_BOOL;
      result->v.boolV = (val[1] == 't') ? TRUE : FALSE;
      break;
    default:
      result->dt = DT_INT;
      result->v.intV = -1;
      break;
    }
  
  return result;
}


RC 
attrOffset (Schema *schema, int attrNum, int *result)
{
  int offset = 0;
  int attrPos = 0;
  
  for(attrPos = 0; attrPos < attrNum; attrPos++)
    switch (schema->dataTypes[attrPos])
      {
      case DT_STRING:
	offset += schema->typeLength[attrPos];
	break;
      case DT_INT:
	offset += sizeof(int);
	break;
      case DT_FLOAT:
	offset += sizeof(float);
	break;
      case DT_BOOL:
	offset += sizeof(bool);
	break;
      }
  
  *result = offset;
  return RC_OK;
}

extern char** strsplit(char* str, char* delim,int *numSplit){
    char** res ;
    char  *part ;
    int i = 0, j = 0;

    char* aux;
    aux = strdup(str);
    part = strtok(aux,delim);
    while(part) { 
	part = strtok(NULL,delim);
	i++ ;
    }
    (*numSplit) = i;
    res = (char **)malloc((i+1)*sizeof(*res));
    free(aux);
    aux = strdup(str);
    part = strtok(aux,delim);
    while(part) { 
	res[j] = strdup(part);
	part = strtok(NULL,delim);
	j++;
    }
    free(aux);
    free(part);
    return res;
}

extern void freeStrArr(char** str,int cnt) 
{
	int i;
	for(i= 0; i<cnt;i++) free(str[i]);
	return;
}

extern void freeValue(Value *val) {
	if(val->dt==DT_STRING) free(val->v.stringV);
	free(val);
}


