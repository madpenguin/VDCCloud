all:	nbd nbd-server

nbd: nbd.c nbd.h
	@gcc -O2 -D_GNU_SOURCE nbd.c -g -o nbd

nbd-server: nbd-server.c nbd.h
	@gcc -O2 -D_GNU_SOURCE nbd-server.c -g -o nbd-server

nbd-client: nbd-client.c nbd.h
	@gcc -O2 -D_GNU_SOURCE nbd-client.c -g -o nbd-client

install:
	git pull
	python setup.py install --record install.txt

uninstall:
	cat install.txt | xargs rm -rf
	
