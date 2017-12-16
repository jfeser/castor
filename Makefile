# CC = clang
# CFLAGS = -Wall -Werror -g
# IFLAGS = -I/Applications/Postgres.app/Contents/Versions/latest/include/
# LDFLAGS = -L/Applications/Postgres.app/Contents/Versions/latest/lib -lpq

# all: builder-column-major.exe builder-row-major.exe

# builder%.exe: builder%.c 
# 	$(CC) $(CFLAGS) $(IFLAGS) $(LDFLAGS) $< -o $@ 

# _build/default/layout.exe: layout.ml
# 	jbuilder build layout.exe

.PHONY: test_eval.exe test
all: $(wildcard bin/*.ml) test

test:
	jbuilder build test/test.exe
	ln -sf _build/default/test/test.exe test.exe

%.exe : bin/%.ml
	jbuilder build bin/$@
	ln -sf _build/default/bin/$@ $@

# test_locality.exe:
# 	jbuilder build bin/test_locality.exe
# 	ln -sf _build/default/bin/test_locality.exe test_locality.exe