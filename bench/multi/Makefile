COMBINE=dune exec --no-build ../../../castor/bin/combine.exe
COMPILE=dune exec --no-build ../../../castor/bin/compile.exe --

queries:
	$(COMBINE) 1-opt.txt 2-opt.txt > 1_2.txt
	$(COMBINE) 2-opt.txt 3-no-opt.txt > 2_3.txt
	$(COMBINE) 3-no-opt.txt 4-opt.txt > 3_4.txt
	$(COMBINE) 4-opt.txt 5-opt.txt > 4_5.txt

1_2: 1_2.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1

2_3: 2_3.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1

3_4: 3_4.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1

4_5: 4_5.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1
