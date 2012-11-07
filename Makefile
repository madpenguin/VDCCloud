all:	nbd2 nbd-server nbd-cache-tool 

nbd2: nbd2.c nbd.h util.c nbd-cache.c nbd-freecache.c
	@gcc -g -pg -O2 -D_GNU_SOURCE nbd2.c util.c nbd-cache.c nbd-freecache.c -o nbd2 -ldb

nbd: nbd.c nbd.h util.c
	@gcc -O2 -D_GNU_SOURCE nbd.c util.c -g -o nbd

nbd-cache-tool: nbd-cache.c nbd.h util.c nbd-cache-tool.c nbd-freecache.c
	@gcc -D_GNU_SOURCE nbd-cache-tool.c nbd-cache.c util.c nbd-freecache.c -g -o nbd-cache-tool -ldb

nbd-server: nbd-server.c nbd.h util.c
	@gcc -O2 -D_GNU_SOURCE nbd-server.c util.c -g -o nbd-server

install:
	git pull
	python setup.py install --record install.txt

uninstall:
	cat install.txt | xargs rm -rf
	
