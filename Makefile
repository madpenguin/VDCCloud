all:	nbd-server.c
	@gcc -O2 -D_GNU_SOURCE nbd-server.c -g -o nbd-server


install:
	git pull
	python setup.py install --record install.txt

uninstall:
	cat install.txt | xargs rm -rf
	
