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

char *hosts[] = {"1.1.1.1","2.2.2.2",NULL};

void main(int argc,char **argv)
{
    int c,status;
	void* data;
	char *dev="/dev/cache/onegig";
	char* options = "elfwrtvs:b:i";
	long block = -1;
	int host=0;
	char data_block[1024];
 	
	status = cacheOpen(dev,hosts);
	if(status==-1) {
		printf("Error opening cache\n");
		exit(1);
	}
	if(status) options="f";
	
    while ((c = getopt (argc, argv, options)) != -1)
    {
        switch(c) {
			case 'e':
				cacheLoad();
				cacheExpire(2);
				break;
			case 'i':
				cacheReIndex();
				break;
			case 'b':
				block = atol(optarg);
				break;
			case 's':
				if(block>0) {
					host = atoi(optarg);
					cacheLoad();				
					if(!cacheFlush(block,host)) printf("Failed!\n");
				}
				else printf("Specify which block first!\n");
				break;
			case 'f':
				cacheFormat(hosts);
				status = 0;
				break;
			case 'w':
				if(block>0) {
					cacheLoad();
					if(!cacheWrite(block,&data_block)) printf("Write Error!\n");
					else printf("Ok\n");
				}
				else printf("Specify which block first!\n");
				break;
			case 'r':
				if(block>0) {
					cacheLoad();
					data = cacheRead(block);
					if(!data) printf("Not found!\n");
					else printf("Ok\n");
				}
				else printf("Specify which block first!\n");
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