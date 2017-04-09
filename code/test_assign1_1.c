#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
static void testPrevNextAppndCapacity(void);

/* main function running all tests */
int
main ()
{
  testName = "";
    
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();
  testPrevNextAppndCapacity();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  free(ph);  
  TEST_DONE();
}

/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Check functionality of appendEmptyBlock, ensureCapacity, readPreviousBlock, readNextBlock, readCurrentBlock, getBlockPos, writeBlock, writeCurrentBlock */
void
testPrevNextAppndCapacity(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph, inputData;
  int i, flag;

  ph = (SM_PageHandle) malloc(PAGE_SIZE);
  inputData = (SM_PageHandle) malloc(PAGE_SIZE);
  testName = "Shikha Functional Test 1 :";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  TEST_CHECK(appendEmptyBlock(&fh));
  ASSERT_TRUE((fh.totalNumPages == 2), "after appending 1 block to new file, there should be 2 pages");

  TEST_CHECK(ensureCapacity(3,&fh));
  ASSERT_TRUE((fh.totalNumPages == 3), "after ensuring capacity for 3 pages, there should be 3 pages in the file");

  for (i=0; i < PAGE_SIZE; i++)
    inputData[i] = '0';
  TEST_CHECK(writeBlock (0, &fh, inputData)); //First page has '0' written to it in each byte
  for (i=0; i < PAGE_SIZE; i++)
    inputData[i] = '1';
  TEST_CHECK(writeBlock (1, &fh, inputData));// Second page has '1' written to it in each byte
  for (i=0; i < PAGE_SIZE; i++)
    inputData[i] = '2';
  TEST_CHECK(writeBlock (2, &fh, inputData));// Third page has '2' written to it in each byte

  TEST_CHECK(readFirstBlock(&fh,ph));
  flag =1;
  for (i=0; i < PAGE_SIZE; i++) flag = flag && (ph[i] == '0');
  ASSERT_TRUE(flag, "Each Byte on first page is '0'");

  TEST_CHECK(readNextBlock(&fh,ph));
  flag =1;
  for (i=0; i < PAGE_SIZE; i++) flag = flag && (ph[i] == '1');
  ASSERT_TRUE(flag, "Each Byte on second page is '1'");

  TEST_CHECK(readNextBlock(&fh,ph));
  flag =1;
  for (i=0; i < PAGE_SIZE; i++) flag = flag && (ph[i] == '2');
  ASSERT_TRUE(flag, "Each Byte on third page is '2'");
  ASSERT_TRUE((getBlockPos(&fh)==2), "Current page position should be 2, which is the third page ");

  TEST_CHECK(readPreviousBlock(&fh,ph));
  flag =1;
  for (i=0; i < PAGE_SIZE; i++) flag = flag && (ph[i] == '1');
  ASSERT_TRUE(flag, "Each Byte on page previous to third page is '1' (2nd page)");
  ASSERT_TRUE((getBlockPos(&fh)==1), "Current page position should be 1, which is the second page ");

  for (i=0; i < PAGE_SIZE; i++)
    inputData[i] = '4';
  TEST_CHECK(writeCurrentBlock (&fh, inputData));// Now 2nd page has '4' written to it in each byte

  TEST_CHECK(readCurrentBlock(&fh,ph));
  flag =1;
  for (i=0; i < PAGE_SIZE; i++) flag = flag && (ph[i] == '4');
  ASSERT_TRUE(flag, "Now Each Byte on first page is '4'");


  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  TEST_DONE();
  free(ph);
  free(inputData);
}


