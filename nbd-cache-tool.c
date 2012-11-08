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

char *hosts[] = {"127.0.0.1","127.0.0.1",NULL};

void main(int argc,char **argv)
{
    int c,status;
	void* data;
	char *dev="/dev/cache/onegig";
	char* options = "gxelfwrts:b:";
	long block = -1;
	int host=0;
	char data_block[4096];
	char buf[1024];
	int i;
 	
	status = cacheOpen(dev,hosts);
	if(status == -1) {
		printf("Error opening cache\n");
		exit(1);
	}
	if(status) options="f";
	
    while ((c = getopt (argc, argv, options)) != -1)
    {
        switch(c) {
			case 'g':
				exit(1); // exit without closing!
			case 'e':
				cacheExpire(2);
				break;
			case 'b':
				block = atol(optarg);
				break;
			case 's':
				if(block>0) {
					host = atoi(optarg);
					if(!cacheFlush(block,host)) printf("Failed!\n");
				}
				else printf("Specify which block first!\n");
				break;
			case 'f':
				cacheFormat(hosts);
				exit(0);
				status = 0;
				break;
			case 'w':
				if(block>0) {
					if(!cacheWrite(block*NCACHE_BSIZE,sizeof(data_block),&data_block)) printf("Write Error!\n");
					else printf("Ok\n");
				}
				else printf("Specify which block first!\n");
				break;
			case 'r':
				if(block>0) {
					if(!cacheRead(block*NCACHE_BSIZE,data_block,sizeof(data_block)))
							printf("Not found!\n");
					else 	printf("Ok\n");
				}
				else printf("Specify which block first!\n");
				break;
			case 'l':
				cacheList();
				break;
			case 't':
				cacheTest();
				break;
			case 'x':
				cacheStats();
				break;			
			default:
				exit(1);
        }
    }
	cacheClose(dev);
}