CC = clang
CFLAGS = -Wall -Werror -g
IFLAGS = -I/Applications/Postgres.app/Contents/Versions/latest/include/
LDFLAGS = -L/Applications/Postgres.app/Contents/Versions/latest/lib -lpq

all: builder

builder: builder.c
	$(CC) $(CFLAGS) $(IFLAGS) $(LDFLAGS) builder.c -o builder

builder.c: _build/default/layout.exe builder.c.tmpl
	_build/default/layout.exe

_build/default/layout.exe: layout.ml
	jbuilder build layout.exe
