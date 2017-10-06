CC = clang
CFLAGS = -Wall -Werror -g
IFLAGS = -I/Applications/Postgres.app/Contents/Versions/latest/include/
LDFLAGS = -L/Applications/Postgres.app/Contents/Versions/latest/lib -lpq

all: builder-column-major.exe builder-row-major.exe

builder%.exe: builder%.c 
	$(CC) $(CFLAGS) $(IFLAGS) $(LDFLAGS) $< -o $@ 

_build/default/layout.exe: layout.ml
	jbuilder build layout.exe
