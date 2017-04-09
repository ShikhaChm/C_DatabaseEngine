#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"

// Bookkeeping for scans
/**
 * This structure does the book keeping for scan operations. The mgmtData stores an RM_MetaScan, that is mainly responsible for the meta data.
 */
typedef struct RM_ScanHandle
{
  /*@{*/
  /** The table that we are scanning using this RM_ScanHandle */
  RM_TableData *rel;
  /** This stores the meta data for scan using RM_MetaScan struct */
  void *mgmtData;
  /*@}*/
} RM_ScanHandle;

/**
 * This structure stores the meta data of a scan handle.
 */
typedef struct RM_MetaScan
{
  /*@{*/
  /** This stores the current position of the RM_ScanHandle in the table  */
  RID iterator;
  /** This stores the condition that each record must satisfy to qualify in the scan*/
  Expr *cond;
  /*@}*/
} RM_MetaScan;

// table and manager
/**
 * This function initilizes a record manager. I didn't find any practical use for it
 */
extern RC initRecordManager (void *mgmtData);
/**
 * This function shuts down a record manager. I didn't find any practical use for it
 */
extern RC shutdownRecordManager ();
/**
 * This function creates a new table . Creates its underlying pagefile. writes the table info on first page, creates second page which is an empty dataPage. On first page (table info), the firstFreePage is initialized to 1 (pointing to the empty data page).
 * @param[in] name Name of the new table 
 * @param[in] schema Schema of the new table. 
 */
extern RC createTable (char *name, Schema *schema);
/**
 * This function opens a table with tablename name. In the process it allocates memory to mgmtData and initializes a buffer_manager of size RM_BUFFER_SIZE.
 * @param[out] rel The name, schema and buffer_manager for the table is stored here 
 * @param[in] name The name of the table that we want to open.
 */
extern RC openTable (RM_TableData *rel, char *name);
/**
 * This function de allocates the the mgmtData and schema of rel, and disconects it to the table filename by shutting down the buffer_manager contained in mgmt data.
 * @param[in] rel The table that we want to close 
e*/
extern RC closeTable (RM_TableData *rel);
/**
 * This function destroys the underlying pageFile
 * @param[in] rel The table that we are trying to delete 
 */
extern RC deleteTable (char *name);
/** 
 * This function returns the total number of records in a table.
 * @param[in] rel The table in which we are looking for records 
 */
extern int getNumTuples (RM_TableData *rel);
/**
 * This function returns the total number of data pages in a table.
 * @param[in] rel The table in which we are looking for pages 
 */
extern int getNumPages (RM_TableData *rel);
/**
 * This function returns the page number of the first page with a free slot in the table. A free page is created using createEmptyPage or as a result of deleting a record in a full page. Whenever a new free page is created (using either methods), the firstFreePage in tableInfo is assigned to nextFreePage on the new freePage. The firstFreePage in table info is then updated to the page number of the new freePage. With this scheme I create a symbolic linked list of free pages in the table with its head indicated by the firstFreePage in table info stored on first page.
 * @param[in] rel The table in which we are looking for the first free page.  
 */
extern int getFirstFreePage (RM_TableData *rel);
/**
 * This function creates an empty data page for a table with given schema. It writes the management and free space information on first TABLE_META_SIZE bytes of this page, and leaves the rest blank.
 * @param[in] schema The schema of the table for which we are creating a new data page 
 */
extern char * createEmptyPage(Schema *schema);

// handling records in a table
/**
 * This function is used to return a new record in the table. The first empty (page, slot) is found and the record is added, and the id of the record is updated to this (page,slot). If there are no such empty (page,slots) then a new page is created where the record is entered (and the first free page in table info on first free page is updated to this new page) 
 * @param[in] rel The table in which we want to insert 
 * @param[inout] record The record which want to insert in the table rel. The RID where the record goes is updated in record->id.  
 */
extern RC insertRecord (RM_TableData *rel, Record *record);
/**
 * This function is used to delete a record from table. The record is found using id.  If the record is not present, it returns a RC_RM_RECORD_NOT_FOUND. 
 * @param[in] rel The table from which we want to delete a record 
 * @param[in] id RID =(page,slot) of the record which we want to delete.
 */
extern RC deleteRecord (RM_TableData *rel, RID id);
/**
 * This function updates a record in the table. The record is found using record->id. If record->id points to an empty slot, a RC_RM_RECORD_NOT_FOUND  error is returned. I am sticking with (update semantics) here and not trying to do an (update or insert)
 * @param[in] rel The table in which we are updating a record. 
 * @param[in] record The record that we are trying to update. 
 */
extern RC updateRecord (RM_TableData *rel, Record *record);
/**
 * This function fetches a record from a table. The record is identified using its RID, and the result is returned in record. This function returns RC_RM_RECORD_NOT_FOUND if the provided RID is empty.
 * @param[in] rel The table from which we are trying to fetch the record. 
 * @param[in] RID The id<page,slot> of the record that we are trying to fetch
 * @param[out] record This is where the record from table is returned (if found, else its is left unallocated)
 *
 */
extern RC getRecord (RM_TableData *rel, RID id, Record **record);

// scans
/**
 * This function marks the begining of a scan procedure. It allocates the mgmtData of scan handle and initializes it using information from RM_TableData
 * @param[in] rel The table that we want to scan
 * @param[in] scan The scan handle that we want to use for scanning rel
 * @param[in] Expr The condition that any record should satisfy to qualify as a result of this scan. If null, all records qualify  
 *
 */
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
/**
 * This function acts like an iterator on each record of the table. After initilization of scan handle using startScan, every time this function is called, the next record in table, that satisfies scan->Expr condition, is returned. The RID of this record is updated in scan->mgmtData using the RM_MetaScan struct. The next time this function is called, iteration begins from this point onwards.
 * @param[in] scan The scan handle that we are iterating on. 
 * @param[out] record Stores the next record satisfying the scan conditions
 */
extern RC next (RM_ScanHandle *scan, Record **record);
/**
 * This function marks the end of a scan operation. It deallocates the mgmtData of the scan handle.
 * @param[in] scan The scan handle whose meta data is to be deallocated. 
 */
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
/**
 * This function computes the size of a non serialized version of the record
 * @param[in] schema The schema of the record for which we are computing the size. 
 */
extern int getRecordSize (Schema *schema);
/**
 * This function computes the size of a record in its serialized string representation. Since data in dataPages are to be written to disk, it needs to be written in serialized form, so that we can read it back when needed. This size computation helps us determine how many records we want to write in a dataPage (if a fixed PAGE_SiZE). A serialized record looks like this :  [1-0] (a:0,b:aaaa,c:3)
 * @param[in] schema The schema of the record for which we are computing the size. 
 */
extern int getSerializedRecordSize (Schema *schema);
/**
 * This function determines how many records we want to write to a given page. It computes the size as (PAGE_SIZE - TABLE_META_SIZE)/(size of serialized record);
 * @param[in] schema The schema of the record for which we are computing this number. 
 */
extern int getNumRecordsPerPage (Schema *schema);
/**
 * This function creates a schema variable from the supplied inputs
 * @param[in] numAttr Number of Attributes in the schema 
 * @param[in] attrNames An array of size numAttr containing names (char*) of each attribute, passed as pointer to pointer to char 
 * @param[in] dataTypes An array of size numAttr containing datatype of each attribute, passed as pointer to DataType 
 * @param[in] typeLength An array of size numAttr containing size of each attribute's DataType, passed as pointer to DataType 
* @param[in] keySize Number of Keys for the table described by the schema 
 * @param[in] dataTypes An array of size keySize containing index of each key attribute in attrNames array, passed as pointer to int
 */
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
/**
 * This function deallocates all memory allotted to a schema
 * @param[in] schema The schema to be freed  
 */
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
/**
 * This function allocates memory for a new record
 * @param[in] record The pointer to pointer of a record that is being allocated.
 * @param[in] schema The schema of the record. 
 */
extern RC createRecord (Record **record, Schema *schema);
/**
 * This function deallocates all memory allotted to a record variable.
 * @param[in] record The record to be freed 
 */
extern RC freeRecord (Record *record);
/**
 * This function gets value of a particular attribute in a record
 * @param[in] record The record whose attribute is to be fetched
 * @param[in] schema The schema of the record whose attribute is to fetched
 * @param[in] attrNum The attribute number of the record which is to fetched
 * @param[out] value The value read from the record's attribute field 
 */
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
/**
 * This function sets value of a particular attribute in a record
 * @param[in] record The record whose attribute is to saved 
 * @param[in] schema The schema of the record whose attribute is to saved 
 * @param[in] attrNum The attribute number of the record which is to saved 
 * @param[in] value The new value to be set in the record's attribute field 
 */
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

#endif // RECORD_MGR_H
