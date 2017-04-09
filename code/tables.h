#ifndef TABLES_H
#define TABLES_H

#include "dt.h"
#include "list.h"
#include "buffer_mgr.h"

// Data Types, Records, and Schemas
/**
 *  An enum to capture different Data Types supported for colums of tables
 */
typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3
} DataType;

/**
 * Value stores data of any one cell <recordNumber, attrNum>. The data can be of any one of the types defined in enum DataType 
 */
typedef struct Value {
  /*@{*/
  /** This variable contains the type of the Value stored*/
  DataType dt;
  /** This union is supposed to contain value in one of the 4 union variables which corresponds to the type contained by dt*/
  union v {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  } v;
  /*@}*/
} Value;

/**
 * Serves as unique id of each record written to the table, and stores the relative position of a record within the table.
 */
typedef struct RID {
  /*@{*/
  /** This variable contains the page number on which a record is stored*/
  int page;
  /** Within a page, this variable contains the relative position of the record*/
  int slot;
  /*@}*/
} RID;

/**
 * A record is any one row of our table. It has a RID id identifier and corresponding data
 */
typedef struct Record
{
  /*@{*/
  /** This variable stores the relative position of a record within the table*/
  RID id;
  /** This contains serialized  data of any one row of table*/
  char *data;
  /*@}*/
} Record;
 
/**
 * Schema contains information of a table schema: its attributes, datatypes, sizes of each datatype, key attributes and number of keys.
 */
typedef struct Schema
{
  /*@{*/
  /** This variable contains the number of columns in our table */
  int numAttr;
  /** This variable contains an array of names of each column*/
  char **attrNames;
  /** This variable contains an array of types of each column*/
  DataType *dataTypes;
  /** This variable contains an array of size of each column */
  int *typeLength;
  /** This variable cotains an array of columns to be considered as keys for our table */
  int *keyAttrs;
  /** This variable contains the number of keys of the table */
  int keySize;
  /*@}*/
} Schema;
 
/**
 * TableData: Management Structure for a Record Manager to handle one relation. 
 */
typedef struct RM_TableData
{
  /*@{*/
  /** This variable contains the name of the table */
  char *name;
  /** This variable contains the schema of the table */
  Schema *schema;
  /** This data is responsible for tracking and managing the table data and free space */
  void *mgmtData;
  /*@}*/
} RM_TableData;

/**
 * RelData  would be responsible for tracking and managing data and free space of table data. I have assumed that for each table, the first page contains schema and some meta info. Second page onwards we have page data. In page data I store 2 things before records. 1) a boolean array of size numRecordsPerPage indicating whether the corresponding slot is empty or full. 2) the page number of next free page
 */
typedef struct RM_RelData
{
  /*@{*/
  /** This variable contains the total number of records in the table*/
  int totalNumberOfRecords;
  /** This variable contains the number of data pages that are present in the table */
  int numDataPages;
  /** This contains the first page number where not all slots are full */
  int firstFreePage;
  /** This contains a buffer manager for reading and writing pages of the table from disk*/
  BM_BufferPool *bm; 
  /*@}*/
} RM_RelData;


/**
 * dynamic string
 */
typedef struct VarString {
  /*@{*/
  /** This variable contains the string variable */
  char *buf;
  /** This variable contains the size of the string variable */
  int size;
  /** This variable cotains the size of the buffer */
  int bufsize;
  /*@}*/
} VarString;

#define MAKE_VARSTRING(var)				\
  do {							\
  var = (VarString *) malloc(sizeof(VarString));	\
  var->size = 0;					\
  var->bufsize = 100;					\
  var->buf = malloc(100);				\
  } while (0)

#define FREE_VARSTRING(var)			\
  do {						\
  free(var->buf);				\
  free(var);					\
  } while (0)

#define GET_STRING(result, var)			\
  do {						\
    result = malloc((var->size) + 1);		\
    memcpy(result, var->buf, var->size);	\
    result[var->size] = '\0';			\
  } while (0)

#define RETURN_STRING(var)			\
  do {						\
    char *resultStr;				\
    GET_STRING(resultStr, var);			\
    FREE_VARSTRING(var);			\
    return resultStr;				\
  } while (0)

#define ENSURE_SIZE(var,newsize)				\
  do {								\
    if (var->bufsize < newsize)					\
    {								\
      int newbufsize = var->bufsize;				\
      while((newbufsize *= 2) < newsize);			\
      var->buf = realloc(var->buf, newbufsize);			\
    }								\
  } while (0)

#define APPEND_STRING(var,string)					\
  do {									\
    ENSURE_SIZE(var, var->size + strlen(string));			\
    memcpy(var->buf + var->size, string, strlen(string));		\
    var->size += strlen(string);					\
  } while(0)

#define APPEND(var, ...)			\
  do {						\
    char *tmp = malloc(10000);			\
    sprintf(tmp, __VA_ARGS__);			\
    APPEND_STRING(var,tmp);			\
    free(tmp);					\
  } while(0)



#define MAKE_STRING_VALUE(result, value)				\
  do {									\
    (result) = (Value *) malloc(sizeof(Value));				\
    (result)->dt = DT_STRING;						\
    (result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
    strcpy((result)->v.stringV, value);					\
  } while(0)


#define MAKE_VALUE(result, datatype, value)				\
  do {									\
    (result) = (Value *) malloc(sizeof(Value));				\
    (result)->dt = datatype;						\
    switch(datatype)							\
      {									\
      case DT_INT:							\
	(result)->v.intV = value;					\
	break;								\
      case DT_FLOAT:							\
	(result)->v.floatV = value;					\
	break;								\
      case DT_BOOL:							\
	(result)->v.boolV = value;					\
	break;								\
      }									\
  } while(0)


// debug and read methods
/**
 * This function serializes the table info part of an RM_TableData to string. The table info refers to the first page on disk for each table, which contains meta information for the table like its schema, first free page etc.
 * @param[in] rel The table for which we want to serialize the table info 
 */
extern char *serializeTableInfo(RM_TableData *rel);
/**
 * This function serializes all the records of a table into one big string. This function is only good for debugging purposes
 * @param[in] rel The table for which we want to serialize the table content
 */
extern char *serializeTableContent(RM_TableData *rel);
/**
 * This function serializes a Schema variable and converts it to an equivalent string representation
 * This function serializes an individual record and converts it to an equivalent string representation
 * @param[in] schema The schema to be serialized  
 */
extern char *serializeSchema(Schema *schema);
/**
 * This function serializes an individual record and converts it to an equivalent string representation
 * @param[in] record The record to be serialized  
 * @param[in] schema The schema of the record
 */
extern char *serializeRecord(Record *record, Schema *schema);
/**
 * This function serializes an individual attribute (column) of a record of a table.
 * @param[in] record The record for which we are serializing a column.
 * @param[in] schema The schema of the record (needed for offset computation)
 * @param[in] attrNum specifies the attribute that we want to serialize 
 */
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
/**
 * This function serializes an individual Value variable. Returns string
 * @param[in] val The value to be serialized  
 */
extern char *serializeValue(Value *val);
/**
 * This function helps computing the offset of an attribute in record->data given its schema
 * @param[in] schema The schema for which we are computing attribute offset in records
 * @param[in] attrNum The schema contains many attributes. attrNum specifies one of them.
 * @param[out] result Contains the memory offset computed for the specified attrNum
 */
extern RC attrOffset (Schema *schema, int attrNum, int *result);
//de-serialization methods
/**
 * Creates Value from a string containing a serialized representation of a value.
 * @param[in] value The string containing a serialized representation of a Value .
 */
extern Value *stringToValue (char *value);
/**
 * This function takes in a serialized reprensetation of schema of a table, and parses out all relevant information contained  and returns the schema.
 * @param[in] schemaLine The serialized string representation of a schema. 
 */
extern Schema *stringToSchema(char *schemaLine);
/**
 * This function takes in a serialized reprensetation of the first meta information of a table, and parses out all relevant information contained in this page. It returns the schema.
 * @param[in] tabInfo Contains the string in first page of a table
 * @param[out] name The name of table as read out from first page
 * @param[out] numTuples The total number of records stored in table as read out from first page.
 * @param[out] numPages The total number of data pages in table as read out from first page.
 * @param[out] firstFreePage The number of first page with free slots in table as read out from first page
 * 
 */
extern Schema *stringToTabInfo(char *tabInfo, char **name,int *numTuples, int *numPages, int *firstFreePage);
/**
 * This function converts a string variable to corresponding record. 
 * @param[in] recordStr The serialized char*  representation of a record
 * @param[in] schema The schema according to which the record is to be parsed 
 */
extern Record *stringToRecord(char *recordStr, Schema *schema);
/**
 * A utility function to free the memory allocated to a Value variable. A simple free(value) is not enough because it contains a char *buff which might have a malloc allocation. This was one the sources of memory leaks in the supplied test cases.
 * @param[in] val The value to be freed 
 */
extern void freeValue(Value *val);
/**
 * A utility function for splitting a string on multiple delimitters. Returns an array of substrings split on delim
 * @param[in] str The string to be split 
 * @param[in] delim The array of delimitters passed as char *
 * @param[out] numSplit The number of components in the resultant split 
 */
extern char** strsplit(char* str, char* delim,int *numSplit);
/**
 * A utility function for freeing an array of strings of length cnt;
 * @param[in] str string array passed as pointer to pointer to char
 * @param[in] cnt  the number of elements to be freed
 */
extern void freeStrArr(char** str,int cnt);

#endif
