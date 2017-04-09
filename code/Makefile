all: clean expr assign4
expr:
	 gcc -o test_expr test_expr.c rm_serializer.c expr.c record_mgr.c buffer_mgr.c list.c buffer_mgr_stat.c storage_mgr.c dberror.c -lm
assign4:
	 gcc -o test_assign4 test_assign4_1.c btree_mgr.c expr.c rm_serializer.c record_mgr.c buffer_mgr.c list.c buffer_mgr_stat.c storage_mgr.c dberror.c -lm 
assign3:
	 gcc -o test_assign3 test_assign3_1.c expr.c rm_serializer.c record_mgr.c buffer_mgr.c list.c buffer_mgr_stat.c storage_mgr.c dberror.c -lm 
assign2:
	 gcc -o test_assign2 test_assign2_1.c buffer_mgr.c list.c buffer_mgr_stat.c storage_mgr.c dberror.c
assign1:
	gcc -o test_assign1 test_assign1_1.c storage_mgr.c dberror.c
docs:
	doxygen doxyConfig; cd docs/latex/ ; make pdf ; cd ../../
clean:
	rm -rf ./test_assign1 ./test_assign2 ./testbuffer.bin ./docs ./test_assign3 ./test_expr ./test_table_r ./test_assign4 testidx
