/*
 *      nbd-cache.c
 *      (c) Gareth Bult 2012
 *
 *	TODO :: Add "sync" to mark block as written to net
 *	TODO :: split tree for "used" and "dirty"
 *	TODO :: Actually read/write data blocks
 *	TODO :: Plan for incremental recovert
 *	TODO :: Format option [clear], need to write zeros into cache
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
#include <syslog.h>
#include <db.h>

#include "nbd.h"

#define FREE	0
#define USED	1
#define DIRTY	2

///////////////////////////////////////////////////////////////////////////////
//
//	Global Variables
//
///////////////////////////////////////////////////////////////////////////////

uint64_t	cache_blocks;	// cache size (raw 512 blocks)
uint64_t	cache_entries;	// entries in cache
uint32_t*	freeq;			// freeq for free blocks
uint32_t*	freeq_next;		// next free block

int 		cache;			// cache file handle
DB*			db;				// BDB in-memory database
DB*			hash_used;		// hash table for used blocks
DB*			hash_dirty;		// hash table for dirty blocks
DBT 		key,val;		//

///////////////////////////////////////////////////////////////////////////////
//
//	cacheOpen	- initialise caching operations
//
//	Cache is opened and handle stored in 'cache'
//	Cache size is read from the underlying device and sets "cache_blocks"
//
//	TODO :: embed magic + cache size info in cache header
//
///////////////////////////////////////////////////////////////////////////////

char* cacheOpen(char* dev)
{
	int ret;
	key.flags = 0;	// make sure flags are null'd
	val.flags = 0;
	//	
	cache = open(dev,O_RDWR|O_EXCL);
	if( cache == -1 ) {
		syslog(LOG_ALERT,"Unable to open Cache (%s), err=%d",dev,errno);
		return False;
	}
	if(ioctl(cache, BLKGETSIZE, &cache_blocks)) {
		syslog(LOG_ALERT,"Unable to read cache size, err=%d\n",errno);
		return False;
	}
	syslog(LOG_INFO,"Cache Open (%s) %lldM\n",dev,(unsigned long long)cache_blocks*512/1024/1024);
	cache_entries = cache_blocks*512 / (1024+2*sizeof(index_entry));
	//
	if( db_create(&db,NULL,0) != 0) {
		close(cache);
		syslog(LOG_ALERT,"Unable to create DB handle");
		return False;
	}	
	if( ret = db->open(db, NULL, NULL, NULL, DB_HASH, DB_CREATE, 0777) != 0) {
		syslog(LOG_ALERT,"Unable to open memory DB, err=%d",errno);
		return False;
	}
	freeq = (uint32_t*)malloc(sizeof(uint32_t)*cache_entries);
	freeq_next = freeq;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheSave		- save the current trees to backing store
//
///////////////////////////////////////////////////////////////////////////////

int cacheSave()
{
	int bytes,ret,slot;
	int meta_size = cache_entries*sizeof(index_entry);
	index_entry* ptr;
	index_entry* index_base = (index_entry*)malloc(meta_size);
	hash_entry* entry;
	DBC *cursor;
	int count = 0;
	
	memset(index_base,0,meta_size);	
	if( db->cursor(db,NULL,&cursor,0) != 0) {
		syslog(LOG_ALERT,"Unable to create cursor in cacheSave");
		return False;
	}
	ret = cursor->c_get(cursor,&key,&val,DB_FIRST);
	while( ret == 0 ) {
		entry		= (hash_entry*)(val.data);
		ptr			= index_base + entry->slot;		
		ptr->dirty 	= entry->dirty;
		ptr->block 	= entry->block;
		ret = cursor->c_get(cursor,&key,&val,DB_NEXT);
		count++;
	}
	if(lseek(cache,0,SEEK_SET)==-1) {
		syslog(LOG_ALERT,"Unable to seek to the beginning of the cache");
		return False;
	}
	bytes = write(cache,index_base,meta_size);
	if(bytes != meta_size) {
		syslog(LOG_ALERT,"Failed to write to cache");
		return False;
	} 
	syslog(LOG_INFO,"Cache saved %d entries, data=%dM, meta=%dM",count,
		   (int)(cache_blocks*512/1024/1024),bytes/1024/1024);		
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheClose		- close the cache device
//
//	This automatically calls 'save' to make sure we keep our place ..
//
///////////////////////////////////////////////////////////////////////////////

void cacheClose(char* dev)
{
	cacheSave();
	free(freeq);
	db->close(db,0);
	syslog(LOG_INFO,"Cache (%s) closed",dev);
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheRead	- read an entry from the cache
//
//	TODO :: try both used and dirty trees
// 	TODO :: get actual data rather than header entries
//
///////////////////////////////////////////////////////////////////////////////

hash_entry* cacheRead(uint64_t block)
{
	int ret;
	
	key.data = &block;
	key.size = sizeof(block);
	
	ret = db->get(db,NULL,&key,&val,0);
	if( ret == DB_NOTFOUND ) return NULL;
	return (hash_entry*)(val.data);
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheClear	- initialise the cache
//
//	TODO :: clear down the data area too!
//
///////////////////////////////////////////////////////////////////////////////

int cacheClear()
{
	int bytes;
	int meta_size = cache_entries*sizeof(index_entry);
	char *blank = (char*)malloc(meta_size);
	
	memset(blank,0,meta_size);
	bytes = write(cache,blank,meta_size);
	if(bytes != meta_size) {
		syslog(LOG_ALERT,"Unable to clear cache header");
		return False;
	}
	syslog(LOG_INFO,"Cache initialised, ready for %d entries\n",(int)cache_entries);
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheLoad	- load the cache in from backing store
//
//	TODO :: split between used / dirty!
//  TODO :: update INFO to include used / dirty!
//
///////////////////////////////////////////////////////////////////////////////

int cacheLoad()
{
	int bytes,ret,slot;
	int meta_size = cache_entries*sizeof(index_entry);

	if(lseek(cache,0,SEEK_SET)==-1) {
		syslog(LOG_ALERT,"Unable to seek BOF in cacheLoad");
		return False;
	}
	index_entry* index_base = (index_entry*)malloc(meta_size);
	index_entry* ptr = index_base;
	bytes = read(cache,ptr,meta_size);
	if( bytes != meta_size ) {
		syslog(LOG_ALERT,"Cache header is wrong size!");
		return False;
	}
	for(slot=0;slot<cache_entries;slot++) {
		if(ptr->dirty) {
			hash_entry* entry = (hash_entry*)malloc(sizeof(hash_entry));
			entry->slot 	= slot;
			entry->block 	= ptr->block;
			entry->dirty 	= ptr->dirty;
			
			val.data = entry;
			val.size = sizeof(hash_entry);
			key.data = &entry->block;
			key.size = sizeof(entry->block);
			
			if(ret = db->put(db,NULL,&key,&val,0) !=0) {
				syslog(LOG_ALERT,"Unable to insert entry into HASH");
				return False;
			}
		} else {
			*freeq_next++ = slot;
		}
		ptr++;
	}
	syslog(LOG_INFO,"Cache loaded, free list size = %ld",freeq_next-freeq);
	free(index_base);	
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheWrite	- write a new entry into the local cache
//
//	TODO :: use dirty tree for writes
//
///////////////////////////////////////////////////////////////////////////////

int cacheWrite(uint64_t block)
{
	int ret;
	hash_entry* entry = (hash_entry*)malloc(sizeof(hash_entry));
	
	entry->slot 	= *--freeq_next;
	entry->block 	= block;
	entry->dirty 	= DIRTY;
	
	val.data = entry;
	val.size = sizeof(hash_entry);
	key.data = &entry->block;
	key.size = sizeof(entry->block);
		
	if(ret = db->put(db,NULL,&key,&val,0) !=0) {
		syslog(LOG_ALERT,"Unable to insert entry into HASH");
		return False;
	}
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheList	- list the contents of the cache
//
//	TODO :: re-arrange for used/dirty
//
///////////////////////////////////////////////////////////////////////////////

int cacheList()
{
	int bytes,ret,slot;
	hash_entry* entry;
	DBC *cursor;	
	
	if( db->cursor(db,NULL,&cursor,0) != 0) {
		syslog(LOG_ALERT,"Unable to allocatoe Cursor!");
		return False;
	}
	ret = cursor->c_get(cursor,&key,&val,DB_FIRST);
	while( ret == 0 ) {
		entry = (hash_entry*)(val.data);
		printf("%08lld - %08lld :: %d\n",
			   (unsigned long long)entry->slot,
			   (unsigned long long)entry->block,
			   entry->dirty);
		ret = cursor->c_get(cursor,&key,&val,DB_NEXT);
	}
	return True;
}
