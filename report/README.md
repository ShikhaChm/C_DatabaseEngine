Compilation Instruction:
  1) Code Compilation:  In the folder DBEngine which contains all the source headers and Makefile run :
	make 
  2) cleanup : Run
        make clean
  3) documention :
        make docs

Binary : make creates test_assign4 binary that runs all the test cases
Documentation: refman.pdf . (If you do make docs, it gets created in docs/latex/refman.pdf). The BTree Manager documentation starts at page 47. This documentation contains all the previous doucmentations as well, hence it might be a bit long. However the index and glossary will make the documentation very very straight forward 


Key Data Structures used( apart from the ones supplied already) :
1) BTNode :  This structure represents an individual node of BTree
2) BTreeMeta:  This structure stores the details of a BTree. It is stored in BTreeHandle->mgmtData 
3) BT_ScanMeta: This structure stores an intermediate stage of the scan operation. It is initialized to represent the first element in BTree (sorted ascending by key).

Both insertion and deletion has been implemented using 2 recurisive functions (each). One for recursively going down and inserting/deleting the [key,rid] in a leafNode. The second recursively travels up from the leafNode upto the root node propagating the impact of overflow-split/underflow-merge (respectively).

For insert : recursiveInsertKey and insertBackPropagate
For delete : recursiveDeleteKey and deleteBackPropagate


