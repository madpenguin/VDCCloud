all:	nbd nbd-server nbd-cache-tool

nbd: nbd.c nbd.h util.c
	@gcc -O2 -D_GNU_SOURCE nbd.c util.c -g -o nbd

nbd-cache-tool: nbd-cache.c nbd.h util.c nbd-cache-tool.c
	@gcc -D_GNU_SOURCE nbd-cache-tool.c nbd-cache.c util.c -g -o nbd-cache-tool -ldb

nbd-server: nbd-server.c nbd.h util.c
	@gcc -O2 -D_GNU_SOURCE nbd-server.c util.c -g -o nbd-server

install:
	git pull
	python setup.py install --record install.txt

uninstall:
	cat install.txt | xargs rm -rf
	
