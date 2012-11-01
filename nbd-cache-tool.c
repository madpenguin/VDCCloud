/*
 *      nbd-cache-tool.c
 *      (c) Gareth Bult 2012
 *
 */

#include <err.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <limits.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <db.h>

#include "nbd.h"

void* cacheRead(uint64_t);

void main(int argc,char **argv)
{
    int c;
	void* data;
	char *dev="/dev/cache/onegig";
 	
	if(!cacheOpen(dev)) {
		printf("Error opening cache\n");
		exit(1);
	}
	
    while ((c = getopt (argc, argv, "lcw:r:tv")) != -1)
    {
        switch(c)
    	{
	    case 'c':
			cacheClear();
			break;
		case 'w':
			cacheLoad();
			cacheWrite(atoi(optarg));	
			break;
		case 'r':
			cacheLoad();
			data = cacheRead(atoi(optarg));
			if(!data) printf("Not found!\n");
			break;
		case 'l':
			cacheLoad();
			cacheList();
			break;
		case 't':
			cacheLoad();
			cacheTest();
			break;
		case 'v':
			cacheLoad();
			cacheVerify();
			break;		
        default:
			exit(1);
        }
    }
	cacheClose(dev);
}