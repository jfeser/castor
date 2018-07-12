.PHONY : all
all :
	jbuilder build @install

.PHONY : coverage
coverage : clean
	BISECT_ENABLE=YES jbuilder runtest
	bisect-ppx-report -I ../_build/default/ -html _coverage/ \
		`find . -name bisect*.out`

.PHONY : clean
clean :
	rm -f `find . -name 'bisect*.out'`
	jbuilder clean

test :
	jbuilder runtest
