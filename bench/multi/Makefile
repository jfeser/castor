COMBINE=dune exec ../../../castor/bin/combine.exe
COMPILE=dune exec ../../../castor/bin/compile.exe

queries:
	$(COMBINE) 1-opt.txt 2-opt.txt > 1_2.txt
	$(COMBINE) 1-opt.txt 2-opt.txt 3-no-opt.txt > 1_2_3.txt
	$(COMBINE) 1-opt.txt 2-opt.txt 3-no-opt.txt 4-opt.txt > 1_2_3_4.txt
	$(COMBINE) 1-opt.txt 2-opt.txt 3-no-opt.txt 4-opt.txt 5-no-opt.txt > 1_2_3_4_5.txt

1_2: 1_2.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -q $< > $@/compile.log 2>&1

1_2_3: 1_2_3.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -q $< > $@/compile.log 2>&1

1_2_3_4: 1_2_3_4.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -q $< > $@/compile.log 2>&1

1_2_3_4_5: 1_2_3_4_5.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -q $< > $@/compile.log 2>&1
