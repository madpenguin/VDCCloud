/*
 *      nbd-cache.c
 *      (c) Gareth Bult 2012
 *
 *	Advances caching model for NBD client / RAID module.
 *	Impelemts LFU model using BDB / secondary index.
 *
 *
 *	TODO :: optimise hash algorithm for block #'s
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
#include <syslog.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <db.h>

#include "nbd.h"

#define FREE	0
#define USED	1

const char *byte_to_binary(int);

///////////////////////////////////////////////////////////////////////////////
//
//	Global Variables
//
///////////////////////////////////////////////////////////////////////////////

uint64_t	cache_blocks;	// cache size (raw 512 blocks)
uint64_t	cache_entries;	// entries in cache
uint64_t	data_offset;	// start of actual data
uint32_t*	freeq;			// freeq for free blocks
uint32_t*	freeq_next;		// next free block
uint8_t		DIRTY;			// bitmap for flushing to hosts

int 		cache;			// cache file handle
DB*			hash_used;		// hash table for used blocks
DB*			hash_dirty;		// hash table for dirty blocks
DB*			hash_index;		// index for hash_used (on "usecount")
DBT 		key,val;		//

cache_header header;

///////////////////////////////////////////////////////////////////////////////
//
//	cacheOpen	- initialise caching operations
//
//	Cache is opened and handle stored in 'cache'
//	Cache size is read from the underlying device and sets "cache_blocks"
//
///////////////////////////////////////////////////////////////////////////////

int cacheInitDB(DB** db)
{
	int ret;
	DB* d;
	
	if( db_create(db,NULL,0) != 0) {
		syslog(LOG_ALERT,"Unable to create DB handle");
		return False;
	}
	d=*db;
	if( ret = d->open(d, NULL, NULL, NULL, DB_HASH , DB_CREATE, 0777) != 0) {
		syslog(LOG_ALERT,"Unable to open memory DB, err=%d",errno);
		return False;
	}
	return True;
}

int cacheInitIndex(DB** db)
{
	int ret;
	DB* d;
	
	if( db_create(db,NULL,0) != 0) {
		syslog(LOG_ALERT,"Unable to create DB handle");
		return False;
	}
	d=*db;
	d->set_flags(d,DB_DUP|DB_DUPSORT);	
	if( ret = d->open(d, NULL, NULL, NULL, DB_BTREE , DB_CREATE, 0777) != 0) {
		syslog(LOG_ALERT,"Unable to open memory DB, err=%d",errno);
		return False;
	}
	return True;
}

int cacheIndexKey(DB* db,const DBT* key,const DBT* val, DBT* idx)
{
	hash_entry* entry = (hash_entry*)val->data;
	//printf("Slot %ld, UseCount %d\n",(unsigned long)entry->slot,entry->usecount);	
	idx->data = &entry->usecount;
	idx->size = sizeof(entry->usecount);
	return 0;
}

int cacheOpen(char* dev,char** hosts)
{
	int i;
	
	key.flags = 0;	// make sure flags are null'd
	val.flags = 0;
	//	
	cache = open(dev,O_RDWR|O_EXCL);
	if( cache == -1 ) {
		syslog(LOG_ALERT,"Unable to open Cache (%s), err=%d",dev,errno);
		return -1;
	}
	if(ioctl(cache, BLKGETSIZE, &cache_blocks)) {
		syslog(LOG_ALERT,"Unable to read cache size, err=%d\n",errno);
		return -1;
	}
	
	if(lseek(cache,0,SEEK_SET)==-1) {
		syslog(LOG_ALERT,"Unable to seek to the beginning of the cache");
		return -1;
	}
	if( read(cache,&header,sizeof(header)) != sizeof(header)) {
		syslog(LOG_ALERT,"Failed to read cache header");
		return -1;
	} 	
	//
	syslog(LOG_INFO,"Cache Open (%s) %lldM\n",dev,(unsigned long long)cache_blocks*512/1024/1024);
	cache_entries = (cache_blocks-1)*512 / (1024+2*sizeof(cache_entry));
	data_offset = NCACHE_HSIZE+cache_entries*sizeof(cache_entry);
	//
	if( cacheInitDB(&hash_used) && cacheInitDB(&hash_dirty) && cacheInitIndex(&hash_index) ){
		freeq = (uint32_t*)malloc(sizeof(uint32_t)*cache_entries);
		freeq_next = freeq;
		if(memcmp(&header.magic,CACHE_MAGIC,sizeof(header.magic))) {
			syslog(LOG_ERR,"Bad Magic in Cache header - reformat this device");
			return 1;
		} else {
			syslog(LOG_INFO,"Cache header :: device size (%lldM)",
				   (unsigned long long)(header.size*512/1024/1024),header.hcount);
			
			if(hash_used->associate(hash_used,NULL,hash_index,cacheIndexKey,0)) {
				syslog(LOG_ALERT,"Failed to create Index DB");
				return False;
			}
			
			i=0;
			DIRTY=1;
			while( hosts[i] ) {
				DIRTY = DIRTY << 1;
				DIRTY += 1;
				if(i<header.hcount) {
					if(!strcmp(inet_ntoa(header.hosts[i]),hosts[i])) syslog(LOG_INFO,"Host # %d :: %s", i,inet_ntoa(header.hosts[i]));
					else {
						syslog(LOG_ERR,"Host # %d MISMATCH :: requested [%s] header says [%s]",i,hosts[i],inet_ntoa(header.hosts[i]));
						return 1;
					}
				}
				i++;
			}
			if(i!=header.hcount) {
				syslog(LOG_ERR,"Host Count is wrong, header specifies %d hosts, requested %d",header.hcount,i);
				return 1;
			}
		}
		return 0;		
	}
	close(cache);
	return -1;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheSave		- save the current trees to backing store
//
///////////////////////////////////////////////////////////////////////////////

int cacheSaveHash(cache_entry* base,DB* db)
{
	int ret;
	int count=0;
	hash_entry* entry;
	cache_entry* ptr;
	DBC *cursor;
	
	if( db->cursor(db,NULL,&cursor,0) != 0) {
		syslog(LOG_ALERT,"Unable to create cursor in cacheSave");
		return -1;
	}
	ret = cursor->c_get(cursor,&key,&val,DB_FIRST);
	while( ret == 0 ) {
		entry			= (hash_entry*)(val.data);
		ptr				= base + entry->slot;		
		ptr->dirty 		= entry->dirty;
		ptr->block 		= entry->block;
		ptr->usecount	= entry->usecount;
		ret = cursor->c_get(cursor,&key,&val,DB_NEXT);
		count++;
	}
	cursor->c_close(cursor);
	return count;
}

int cacheSave()
{
	int bytes,ret,slot;
	int meta_size = cache_entries*sizeof(cache_entry);
	cache_entry* index_base = (cache_entry*)malloc(meta_size);

	memset(index_base,0,meta_size);
	
	int used = cacheSaveHash(index_base,hash_used);
	int dirty = cacheSaveHash(index_base,hash_dirty);
	
	if(lseek(cache,NCACHE_HSIZE,SEEK_SET)==-1) {
		syslog(LOG_ALERT,"Unable to seek to the beginning of the cache");
		return False;
	}
	bytes = write(cache,index_base,meta_size);
	if(bytes != meta_size) {
		syslog(LOG_ALERT,"Failed to write to cache");
		return False;
	} 
	syslog(LOG_INFO,"Cache save :: %d used, %d dirty, data=%dM, meta=%dM",
		   used,dirty,(int)(cache_blocks*512/1024/1024),bytes/1024/1024);
	
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
	hash_used->close(hash_used,0);
	hash_dirty->close(hash_dirty,0);
	free(freeq);
	syslog(LOG_INFO,"Cache (%s) closed",dev);
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheFormat	- initialise the cache
//
///////////////////////////////////////////////////////////////////////////////

int cacheFormat(char** hosts)
{
	char			buffer[NCACHE_CSIZE];
	uint64_t 		left_to_write = cache_blocks * 512;
	int				cycles = left_to_write / NCACHE_CSIZE;
	int 			increment = cycles / 40;
	uint64_t 		block;
	int 			count,i,size;
	
	//goto skip;
	
	if(lseek(cache,0,SEEK_SET)==-1) {		
		syslog(LOG_ALERT,"Seek error, err=%d",errno);	
		return False;					
	}
	memset(&buffer,0,sizeof(buffer));
	printf("Formatting Cache Device (%lldM)\n",(unsigned long long)(cache_blocks*512/1024/1024));
	printf("["); for(i=0;i<40;i++) printf(" "); printf("]\r\%c[C",27);
	
	count=0;
	while( left_to_write > 0 )
	{
		size = left_to_write > NCACHE_CSIZE?NCACHE_CSIZE:left_to_write;
		if( write(cache,buffer,size) != size) {
			printf("\nWrite Error, errno=%d\n",errno);
			exit(1);
		}
		left_to_write -= size;
		count++;
		if(count==increment) {
			printf("."); fflush(stdout);
			count=0;
		}
	}
	printf("\nWriting header...");
	fflush(stdout);
	
skip:
	//
	//	Write cache header
	//
	if(lseek(cache,0,SEEK_SET)==-1) {		
		syslog(LOG_ALERT,"Seek error, err=%d",errno);	
		return False;					
	}
	memcpy(&header.magic,CACHE_MAGIC,sizeof(header.magic));
	header.size = cache_blocks;
	
	i=0;
	while( hosts[i] && i<sizeof(header.hosts) ) {
		inet_aton(hosts[i],&header.hosts[i]);
		i++;
	}
	header.hcount = i;
	if( write(cache,&header,NCACHE_HSIZE) != NCACHE_HSIZE) {
		syslog(LOG_ALERT,"Write error, err=%d",errno);	
		return False;					
	}
	syslog(LOG_INFO,"Cache initialised, ready for %d entries\n",(int)cache_entries);
	printf("Ok\n");
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheLoad	- load the cache in from backing store
//
///////////////////////////////////////////////////////////////////////////////

int cacheLoad()
{
	int bytes,ret,slot;
	int meta_size = cache_entries*sizeof(cache_entry);
	int used=0;
	int dirty=0;
	DB* db;

	if(lseek(cache,NCACHE_HSIZE,SEEK_SET)==-1) {
		syslog(LOG_ALERT,"Unable to seek BOF in cacheLoad");
		return False;
	}
	cache_entry* index_base = (cache_entry*)malloc(meta_size);
	cache_entry* ptr = index_base;
	bytes = read(cache,ptr,meta_size);
	if( bytes != meta_size ) {
		syslog(LOG_ALERT,"Cache header is wrong size!");
		return False;
	}
	for(slot=0;slot<cache_entries;slot++) {
		if(!ptr->dirty) {
			*freeq_next++ = slot;
			ptr++;
			continue;
		}
		switch(ptr->dirty) {
			case USED:
				db = hash_used;
				used++;
				break;
			default:
				db = hash_dirty;
				dirty++;
				break;
		}
		hash_entry* entry = (hash_entry*)malloc(sizeof(hash_entry));
		entry->slot 	= slot;
		entry->block 	= ptr->block;
		entry->dirty 	= ptr->dirty;
		entry->usecount	= ptr->usecount;
			
		val.data = entry;
		val.size = sizeof(hash_entry);
		key.data = &entry->block;
		key.size = sizeof(entry->block);
			
		if(ret = db->put(db,NULL,&key,&val,0) !=0) {
			syslog(LOG_ALERT,"Unable to insert entry into HASH");
			return False;
		}
		ptr++;
	}
	syslog(LOG_INFO,"Loaded %d used, %d dirty, free list size = %ld",used,dirty,freeq_next-freeq);
	free(index_base);	
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheUse	- increment the use-count for a given block
//
///////////////////////////////////////////////////////////////////////////////

void cacheUse(uint64_t block)
{
	int useCount(DB* db)
	{
		int ret;
		hash_entry* entry;
		ret = db->get(db,NULL,&key,&val,0);
		if(ret == DB_NOTFOUND) return False;
		entry = (hash_entry*)(val.data);
		entry->usecount++;
		if( db->put(db,NULL,&key,&val,0) !=0) {
			syslog(LOG_ALERT,"Error updating USE count");
			return False;
		}
		return True;		
	}	
	key.data = &block;
	key.size = sizeof(block);
	if(useCount(hash_used)) return;
	if(useCount(hash_dirty)) return;
	syslog(LOG_ERR,"Attempt to increment use count for uncached block [%lld]",(unsigned long long)block);
}
	
///////////////////////////////////////////////////////////////////////////////
//
//	cacheRead	- read an entry from the cache
//
///////////////////////////////////////////////////////////////////////////////

void* cacheRead(uint64_t block)
{
	int ret;
	hash_entry* entry;
	void* buffer = (void*)malloc(NCACHE_ESIZE);
	
	key.data = &block;
	key.size = sizeof(block);
	
	if( ret = hash_used->get(hash_used,NULL,&key,&val,0) == DB_NOTFOUND ) {
		if( ret = hash_dirty->get(hash_dirty,NULL,&key,&val,0) == DB_NOTFOUND ) return NULL;
	}
	if(ret !=0) {
		syslog(LOG_ALERT,"Read Error on memory DB");
		return NULL;
	}
	entry = (hash_entry*)(val.data);
	//
	SEEK_BLOCK(entry->slot,"READ");
	READ_BLOCK(entry->slot,buffer);
	cacheUse(block);
	return (buffer+sizeof(cache_entry));
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheWrite	- write a new entry into the local cache
//
///////////////////////////////////////////////////////////////////////////////

int cacheWrite(uint64_t block, void* data)
{
	hash_entry* entry = NULL;
	void* buffer = (void*)malloc(NCACHE_ESIZE);
	cache_entry *index = (cache_entry*)buffer;
	DB* db = hash_used;
	int ret;
	
	key.data = &block;
	key.size = sizeof(block);
	
	ret = db->get(db,NULL,&key,&val,0);
	if( ret != 0 ) {
		db = hash_dirty;
		ret = db->get(db,NULL,&key,&val,0);
		if( ret != 0) {
			entry = (hash_entry*)malloc(sizeof(hash_entry));
			entry->slot 	= *--freeq_next;
			entry->block 	= block;
			entry->usecount	= 0;
			val.data = entry;
			val.size = sizeof(hash_entry);		
		}
	}
	if(!entry) entry = (hash_entry*)val.data;
	entry->usecount++;
	entry->dirty = DIRTY;	
	if( db != hash_dirty ) {
		db->del(db,NULL,&key,0);
	}
	if( hash_dirty->put(hash_dirty,NULL,&key,&val,0) !=0) {
		syslog(LOG_ALERT,"Unable to insert entry into HASH");
		return False;
	}
	//
	index->block = block;
	index->dirty = DIRTY;
	index->usecount = entry->usecount;
	memcpy(buffer+sizeof(cache_entry),data,NCACHE_BSIZE);
	//
	SEEK_BLOCK(entry->slot,"WRITE");
	WRITE_BLOCK(entry->slot,buffer);
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheList	- list the contents of the cache
//
///////////////////////////////////////////////////////////////////////////////

int cacheListHash(DB *db,char *title)
{
	int bytes,ret,slot;
	hash_entry* entry;
	DBC *cursor;	

	if( db->cursor(db,NULL,&cursor,0) != 0) {
		syslog(LOG_ALERT,"Unable to allocatoe Cursor!");
		return False;
	}
	ret = cursor->c_get(cursor,&key,&val,DB_FIRST);
	printf("%s entries ...\n",title);
	printf("+----------+----------+----+--------+\n");
	printf("| %8s | %8s | %2s | %-6s |\n","Slot","Block","Fl","UseCnt");
	printf("+----------+----------+----+--------+\n");	
	while( ret == 0 ) {
		entry = (hash_entry*)(val.data);
		printf("| %8lld | %8lld | %2d | %6d |\n",
			   (unsigned long long)entry->slot,
			   (unsigned long long)entry->block,
			   entry->dirty, entry->usecount);
		ret = cursor->c_get(cursor,&key,&val,DB_NEXT);
	}
	cursor->c_close(cursor);
	printf("+----------+----------+----+--------+\n");	
	return True;
}

int cacheList()
{
	return cacheListHash(hash_index,"Used") && cacheListHash(hash_dirty,"Dirty");	
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheFlush - mark a dirty block as flushed
//
///////////////////////////////////////////////////////////////////////////////

int cacheFlush(uint64_t block,int host)
{
	hash_entry* entry;
	key.data = &block;
	key.size = sizeof(block);
	uint8_t mask = 254;
	
	if((host<1)||(host>header.hcount)) {
		syslog(LOG_ERR,"Invalid host number (%d) in flush",host);
		return False;
	}
	if( hash_dirty->get(hash_dirty,NULL,&key,&val,0) !=0 ) {
		syslog(LOG_ERR,"Attempt to flush uncached block [%lld]",(unsigned long long)block);
		return False;
	}
	entry = (hash_entry*)(val.data);
	//
	while( host-- ) {
		mask = mask << 1;
		mask += 1;
	}
	entry->dirty = entry->dirty & mask;
	//
	if(entry->dirty == USED) {
		if( hash_dirty->del(hash_dirty,NULL,&key,0) != 0 ) {
			syslog(LOG_ALERT,"Error removing block to dirty list");
			return False;
		}
		if( hash_used->put(hash_used,NULL,&key,&val,0) !=0) {
			syslog(LOG_ALERT,"Error moving block to used list");
			return False;
		}
		// Update on disk !!
		cache_entry index;
		index.block = block;
		index.dirty = USED;
		index.usecount = entry->usecount;
		SEEK_BLOCK(entry->slot,"WRITE");
		WRITE_INDEX(entry->slot,&index);
		return True;
	}
	if( hash_dirty->put(hash_dirty,NULL,&key,&val,0) !=0) {
		syslog(LOG_ALERT,"Error updating dirty flag");
		return False;
	}
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheReIndex	- rebuild the cache Index from a full data scan
//
///////////////////////////////////////////////////////////////////////////////

int cacheReIndex()
{
	char			buffer[NCACHE_ESIZE];
	int 			increment = cache_entries / 40;
	int 			count,i,size;
	uint32_t		slot=0;
	cache_entry*	index = (cache_entry*)buffer;
	DB*				db;
	int				used = 0,dirty = 0;
	
	SEEK_BLOCK(0,"REBUILD");

	printf("Rebuilding Index for Cache Device (%lldM)\n",(unsigned long long)(cache_blocks*512/1024/1024));
	printf("["); for(i=0;i<40;i++) printf(" "); printf("]\r\%c[C",27);
	
	count=0;
	while( slot < cache_entries )
	{
		if( read(cache,&buffer,sizeof(buffer)) != sizeof(buffer)) {
			printf("\nRead Error, errno=%d\n",errno);
			return False;
		}
		count++;
		if(count==increment) {
			printf("."); fflush(stdout);
			count=0;
		}		
		if(!index->dirty) {
			*freeq_next++ = slot++;
			continue;
		}	
		syslog(LOG_INFO,"Slot=%ld, Block=%lld, Dirty=%d, Use=%ld",(unsigned long)slot,(unsigned long long)index->block,index->dirty,(unsigned long)index->usecount);

		switch(index->dirty) {
			case USED:
				db = hash_used; used++;
				break;
			default:
				db = hash_dirty; dirty++;
				break;
		}
		hash_entry* entry = (hash_entry*)malloc(sizeof(hash_entry));
		entry->slot 	= slot;
		entry->block 	= index->block;
		entry->dirty 	= index->dirty;
		entry->usecount	= index->usecount;
			
		val.data = entry;
		val.size = sizeof(hash_entry);
		key.data = &entry->block;
		key.size = sizeof(entry->block);
			
		if(db->put(db,NULL,&key,&val,0) !=0) {
			syslog(LOG_ALERT,"Unable to insert entry into HASH");
			return False;
		}		
		slot++;
	}
	printf("\nOk\n");
	syslog(LOG_INFO,"Loaded %d used, %d dirty, free list size = %ld",used,dirty,freeq_next-freeq);	
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheExpire	- Expire entries based on LRU
//
///////////////////////////////////////////////////////////////////////////////

int cacheExpire(int units)
{
	DBC *cursor;
	uint32_t slots[100];
	uint32_t next_slot=0;
	hash_entry* entry;
	int ret;
	int count = units;

	if( hash_index->cursor(hash_index,NULL,&cursor,0) != 0) {
		syslog(LOG_ALERT,"Unable to allocate Cursor!");
		return False;
	}
	while( count-- ) {
		if( (ret = cursor->c_get(cursor,&key,&val,DB_FIRST)) !=0 ) break;
		entry = (hash_entry*)(val.data);
		printf("Expiring %08lld - %08lld :: %d :: Use=%d\n",
			   (unsigned long long)entry->slot,
			   (unsigned long long)entry->block,
			   entry->dirty, entry->usecount);

		*freeq_next++ = entry->slot;
		key.data = &entry->block;
		key.size = sizeof(entry->block);
		if( (ret = hash_used->del(hash_used,NULL,&key,0)) !=0 ) {
			syslog(LOG_ALERT,"Error expiring key from DB");
			break;
		}
	}
	cursor->c_close(cursor);
	if( !units && !ret) return True;
	syslog(LOG_ALERT,"Only able to expire %d blocks (of %d)",count,units);
	return False;
}

///////////////////////////////////////////////////////////////////////////////

void cacheTest()
{
	int fd = open("/etc/passwd",O_RDONLY);
	if(fd<1) {
		printf("Error opening file, err=%d\n",errno);
		return;
	}
	
	char buffer[NCACHE_BSIZE];
	int block=10;
	int bytes;
	
	do {
		memset(buffer,0,sizeof(buffer));
		bytes = read(fd,&buffer,sizeof(buffer));
		if(bytes>0) {
			printf("Write=%d,%d\n",cacheWrite(block++,buffer),bytes);
		}
	} while (bytes>0);
}

void cacheVerify()
{
	void* data;
	int block;
	
	for(block=10;block<13;block++) {
		data = cacheRead(block);
		printf("%s",(char*)data);
	}
}