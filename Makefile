BUILD_DIR=../_build/default/

.PHONY : all
all :
	dune build @install

.PHONY : coverage
coverage : clean
	BISECT_ENABLE=YES dune runtest
	bisect-ppx-report -I $(BUILD_DIR) -html _coverage/ \
		`find $(BUILD_DIR)/castor -name bisect*.out`
	open _coverage/index.html

.PHONY : clean
clean :
	rm -f `find $(BUILD_DIR)/castor -name bisect*.out`
	dune clean

.PHONY : test
test :
	dune runtest
