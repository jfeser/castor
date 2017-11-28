# CC = clang
# CFLAGS = -Wall -Werror -g
# IFLAGS = -I/Applications/Postgres.app/Contents/Versions/latest/include/
# LDFLAGS = -L/Applications/Postgres.app/Contents/Versions/latest/lib -lpq

# all: builder-column-major.exe builder-row-major.exe

# builder%.exe: builder%.c 
# 	$(CC) $(CFLAGS) $(IFLAGS) $(LDFLAGS) $< -o $@ 

# _build/default/layout.exe: layout.ml
# 	jbuilder build layout.exe

.PHONY: test_locality.exe

all: test_locality.exe

test_locality.exe:
	jbuilder build bin/test_locality.exe
	ln -sf _build/default/bin/test_locality.exe test_locality.exe
