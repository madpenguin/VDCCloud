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

void main(int argc,char **argv)
{
    int c;
	hash_entry* entry;
	char *dev="/dev/cache/onegig";
 	
	if(!cacheOpen(dev)) {
		printf("Error opening cache\n");
		exit(1);
	}
	
    while ((c = getopt (argc, argv, "lcw:r:")) != -1)
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
			entry = cacheRead(atoi(optarg));
			if(!entry) printf("Not found!\n");
			else printf("Entry=[%lld:%lld:%d]\n",
				(unsigned long long)entry->block,
				(unsigned long long)entry->slot,
				entry->dirty);
			break;
		case 'l':
			cacheLoad();
			cacheList();
			break;
        default:
			exit(1);
        }
    }
	cacheClose(dev);
}