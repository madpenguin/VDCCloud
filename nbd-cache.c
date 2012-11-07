/*
 *      nbd-cache.c
 *      (c) Gareth Bult 2012
 *
 *	Advances caching model for NBD client / RAID module.
 *	Impelemts LFU model using BDB / secondary index.
 *
 *  TODO :: Fix trim, it's not working
 *  TODO :: Fix to work with block size > 1024
 *  TODO :: seek seems to be causing a major slowdown - remove!
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

//uint64_t	cache_blocks;	// cache size (raw 512 blocks)
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

uint64_t	cache_ptr;		// current SEEK pointer for cache
uint64_t	cache_tmp;		// 

cache_header header;

	struct {
		
		char*		name;
		uint64_t	size;
		uint64_t	ssize;
		
	} cache_device;


///////////////////////////////////////////////////////////////////////////////
//
//	cacheTRIM	- issue an SSD TRIM request
//
///////////////////////////////////////////////////////////////////////////////	

void cacheTRIM(uint64_t start,uint64_t count)
{
	uint64_t	range[2],end;
	
	range[0] = data_offset+start; //block*NCACHE_BSIZE;
	range[1] = range[0]+(NCACHE_BSIZE * count);

	printf("TRIM1 Start=%lld,End=%lld\n",(unsigned long long)range[0],(unsigned long long)range[1]);
	
	range[0] = (range[0] + cache_device.ssize - 1) & ~(cache_device.ssize - 1);
	range[1] &= ~(cache_device.ssize - 1);

	printf("TRIM2 Start=%lld,End=%lld\n",(unsigned long long)range[0],(unsigned long long)range[1]);
	
	/* is the range end behind the end of the device ?*/
	end = range[0] + range[1];
	if(end < range[0] || end > cache_device.size) range[1] = cache_device.size - range[0];
	
	if( ioctl(cache,BLKDISCARD, &range) == -1 ) {
		syslog(LOG_ALERT,"TRIM FAILED! [%d]",errno);
		return;
	}
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheOpen	- initialise caching operations
//
//	Cache is opened and handle stored in 'cache'
//	Cache size is read from the underlying device and sets "cache_blocks"
//
///////////////////////////////////////////////////////////////////////////////

int cacheOpen(char* dev,char** hosts)
{
	//
	//	cacheIndexKey - generate index key for secondary index (by usecount)
	//
	int cacheIndexKey(DB* db,const DBT* key,const DBT* val, DBT* idx)
	{
		hash_entry* entry = (hash_entry*)val->data;
		idx->data = &entry->usecount;
		idx->size = sizeof(entry->usecount);
		return 0;
	}
	//
	//	cacheHashFn - optimised hashing function for block numbers
	//
	uint32_t cacheHashFn(DB *db,const void *bytes, uint32_t length)
	{
		return (uint32_t)*(uint64_t*)bytes;
	}
	//
	//	cacheInitDB - initialise a HASH DB
	//
	int cacheInitDB(DB** db)
	{
		int ret;
		DB* d;
	
		if( db_create(db,NULL,0) != 0) {
			syslog(LOG_ALERT,"Unable to create DB handle");
			return False;
		}
		d=*db;
		d->set_h_hash(d,cacheHashFn);
		d->set_cachesize(d,0,cache_device.size*CACHE_FACTOR,0);
		d->set_h_nelem(d,cache_entries);
		d->set_pagesize(d,512);
		if( ret = d->open(d, NULL, NULL, NULL, DB_HASH , DB_CREATE, 0777) != 0) {
			syslog(LOG_ALERT,"Unable to open memory DB, err=%d",errno);	
			return False;
		}
		syslog(LOG_INFO,"Allocated %.2fM to BDB Hash",cache_device.size*CACHE_FACTOR/1024/1024);
		return True;
	}
	//
	//	cacheInitIndex - initialise a BTREE DB
	//
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
	//
	memset(&key,0,sizeof(key));
	memset(&val,0,sizeof(val));
	//	
	cache = open(dev,O_RDWR|O_EXCL);
	if( cache == -1 ) {
		syslog(LOG_ALERT,"Unable to open Cache (%s), err=%d",dev,errno);
		return -1;
	}
	cache_ptr = -1;
	if(ioctl(cache,BLKGETSIZE64,&cache_device.size)==-1) {
		syslog(LOG_ALERT,"Error reading cache device size, err=%d",errno);
		return -1;
	}
	if(ioctl(cache,BLKSSZGET,&cache_device.ssize)==-1) {
		syslog(LOG_ALERT,"Error reading cache sector size, err=%d",errno);
		return -1;
	}
	cache_entries 	= (cache_device.size-NCACHE_HSIZE) / (NCACHE_BSIZE+2*sizeof(cache_entry));
	data_offset 	= NCACHE_HSIZE+cache_entries*sizeof(cache_entry);
	READ_HEADER(cache,header);
	
	if( !cacheInitDB(&hash_used) || !cacheInitDB(&hash_dirty) || !cacheInitIndex(&hash_index) ){
		return -1;
	}
	//if(hash_used->associate(hash_used,NULL,hash_index,cacheIndexKey,0)) {
	//	syslog(LOG_ALERT,"Failed to create Index DB");
	//	return -1;
	//}
	//
	if(memcmp(&header.magic,CACHE_MAGIC,sizeof(header.magic))) {
		syslog(LOG_ERR,"Bad Magic in Cache header - reformat this device");
		return 1;
	}
	//
	freeq = (uint32_t*)malloc(sizeof(uint32_t)*cache_entries);
	freeq_next = freeq;
	//		
	syslog(LOG_INFO,"Opening (%s), size (%lldM), entries (%ld)",dev,(unsigned long long)cache_device.size/1024/1024,(unsigned long)cache_entries);
	//
	int i=0;
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
	//
	if(header.open)
			cacheReIndex();
	else	cacheLoad();
	//
	return 0;		
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheSave		- save the current trees to backing store
//
///////////////////////////////////////////////////////////////////////////////

int cacheSave()
{
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
			//syslog(LOG_INFO,"Slot=%d",entry->slot);			
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
	free(index_base);
	if(bytes != meta_size) {
		syslog(LOG_ALERT,"Failed to write to cache");
		return False;
	} 
	syslog(LOG_INFO,"Cache save :: %d used, %d dirty, data=%dM, meta=%dM",
		   used,dirty,(int)(cache_device.size/1024/1024),bytes/1024/1024);
	
	return True;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheClose		- close the cache device
//
//	This automatically calls 'save' to make sure we keep our place ..
//
///////////////////////////////////////////////////////////////////////////////

int cacheClose(char* dev)
{
	if(header.open) {	
		cacheSave();
		hash_used->close(hash_used,0);
		hash_dirty->close(hash_dirty,0);
		free(freeq);
		syslog(LOG_INFO,"Cache (%s) closed",dev);
		header.open = 0;
		WRITE_HEADER(cache,header);	
		close(cache);
	}
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheFormat	- initialise the cache
//
///////////////////////////////////////////////////////////////////////////////

int cacheFormat(char** hosts)
{
	char			buffer[NCACHE_CSIZE];
	uint64_t 		left_to_write = cache_device.size;
	int				cycles = left_to_write / NCACHE_CSIZE;
	int 			increment = cycles / 40;
	uint64_t 		block;
	int 			count,i,size;
	
	
	
	if(lseek(cache,0,SEEK_SET)==-1) {		
		syslog(LOG_ALERT,"Seek error, err=%d",errno);	
		return False;					
	}
	memset(&buffer,0,sizeof(buffer));
	printf("Formatting Cache Device (%lldM)\n",(unsigned long long)(cache_device.size/1024/1024));
	printf("["); for(i=0;i<40;i++) printf(" "); printf("]\r\%c[C",27);
	
	//for(block=0;block<max_blocks;block++) cacheTRIM(block);	
	
	//goto skip;	
	
	count=0;
	block=0;
	while( left_to_write > 0 )
	{
		size = left_to_write > NCACHE_CSIZE?NCACHE_CSIZE:left_to_write;
		//cacheTRIM(block++);
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
	printf("\nFlushing...\n");
	fflush(stdout);
	
//skip:	
	//
	//	Write cache header
	//
	//if(lseek(cache,0,SEEK_SET)==-1) {		
	//	syslog(LOG_ALERT,"Seek error, err=%d",errno);	
	//	return False;					
	//}
	i=0;
	while( hosts[i] && i<sizeof(header.hosts) ) {
		inet_aton(hosts[i],&header.hosts[i]);
		i++;
	}
	memcpy(&header.magic,CACHE_MAGIC,sizeof(header.magic));
	header.hcount = i;
	header.size = NCACHE_BSIZE;

	WRITE_HEADER(cache,header);
	syslog(LOG_INFO,"Cache initialised, ready for %d entries\n",(int)cache_entries);
	
	printf("Header Information:\n");
	printf("Magic ... "); for(i=0;i<sizeof(header.magic);i++) printf("%c",header.magic[i]); printf("\n");
	printf("Size .... %lldM\n",(unsigned long long)header.size/1024/1024*512);
	printf("Hosts ... %d\n",header.hcount);
	printf("Open .... %d\n",header.open);
	printf("ReIndex . %d\n",header.reindex);
	for(i=0;i<header.hcount;i++) {
		printf(  "Host %i - %s\n",i,inet_ntoa(header.hosts[i]));
	}
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
	hallocLoad(ptr,cache_entries);
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
		hash_entry entry;
		entry.slot 		= slot;
		entry.block 	= ptr->block;
		entry.dirty 	= ptr->dirty;
		entry.usecount	= ptr->usecount;
			
		val.data = &entry;
		val.size = sizeof(hash_entry);
		key.data = &entry.block;
		key.size = sizeof(entry.block);
			
		if(ret = db->put(db,NULL,&key,&val,0) !=0) {
			syslog(LOG_ALERT,"Unable to insert entry into HASH");
			return False;
		}
		ptr++;
	}
	syslog(LOG_INFO,"Loaded %d used, %d dirty, free list size = %ld",used,dirty,freeq_next-freeq);
	free(index_base);
	header.open = 1;		
	WRITE_HEADER(cache,header);
}
	
///////////////////////////////////////////////////////////////////////////////
//
//	cacheRead	- read an entry from the cache
//
///////////////////////////////////////////////////////////////////////////////

void* cacheRead(uint64_t off,int len)
{
	static char buffer[NCACHE_ESIZE];
	int ret;
	hash_entry* entry;
	uint64_t block = off/NCACHE_BSIZE;
	DB* db = hash_used;
	
	key.data = &block;
	key.size = sizeof(block);
	
	ret = db->get(hash_used,NULL,&key,&val,0);
	if( ret != 0 ) {
		db = hash_dirty;
		ret = db->get(hash_dirty,NULL,&key,&val,0);
		if( ret != 0 ) {
			syslog(LOG_ALERT,"Block not in cache");
			return NULL;
		}
	}
	entry = (hash_entry*)(val.data);
	entry->usecount++;
	
	DATA_SEEK(entry->slot,"READ");
	READ_BLOCK(entry->slot,buffer);
	SAVE_CACHE(db);
	
	return (buffer+sizeof(cache_entry));
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheWrite	- write a new entry into the local cache
//
///////////////////////////////////////////////////////////////////////////////

int gcount=150;

void allocateFree(uint64_t *slot,int *count)
{
	syslog(LOG_INFO,"alloc :: request for %d",*count);
	*slot = gcount++;
	*count = 1;
}

void cacheAlignBlock(int *len)
{
	int size = (*len/NCACHE_BSIZE)*NCACHE_BSIZE;
	if( size != *len ) *len = size + NCACHE_BSIZE;
}

int cacheWrite(uint64_t off, int len, void* data)
{
	hash_entry 		new_entry,*entry;
	char*			buffer,*ptr;
	DB* 			db;
	int				count,ret,siz;
	cache_entry		*iptr;
	uint64_t		block = off/NCACHE_BSIZE;
	uint32_t		slot;

	cacheAlignBlock(&len);	
			
	syslog(LOG_INFO,"cacheWrite :: off=%lld, len=%d, data=%llx",(unsigned long long)off,len,(unsigned long long)data);
	
	hallocBegin();
	while( len > 0) {
		count = len/NCACHE_BSIZE;
		//allocateFree(&slot,&count);
		hallocAllocate(&slot,&count);
		ptr = buffer = (char*)malloc(count*NCACHE_ESIZE);
		len -= count*NCACHE_BSIZE;
		
		DATA_SEEK(slot,"WRITE");
		while(count--) {
			syslog(LOG_INFO,"Loop on slot %d, block %lld",(int)slot,(unsigned long long)block);
			db = hash_used;
			key.data = &block;
			key.size = sizeof(block);		
			ret = db->get(db,NULL,&key,&val,0);
			if( ret != 0 ) {
				db = hash_dirty;
				ret = db->get(db,NULL,&key,&val,0);
			}
			if(ret == 0) {	
				if( db != hash_dirty ) {
					db->del(db,NULL,&key,0);
				}
				// Free old slot !
				entry = (hash_entry*)val.data;
				syslog(LOG_INFO,"Need to free slot (%d)",entry->slot);
				hallocFree(entry->slot);
			} else {
				entry = &new_entry;
				entry->block 	= block;
				entry->usecount = 0;
				val.data 		= entry;
				val.size 		= sizeof(new_entry);
			}
			entry->slot  = slot;			
			entry->dirty = DIRTY;
			entry->usecount++;
			SAVE_CACHE(hash_dirty);
			
			iptr = (cache_entry*)ptr;
			iptr->block 	= block;
			iptr->dirty 	= DIRTY;
			iptr->usecount	= entry->usecount;
			ptr += sizeof(cache_entry);
			memcpy(ptr,data,NCACHE_BSIZE);
			ptr  += NCACHE_BSIZE;
			data += NCACHE_BSIZE;
			block++;
			slot++;
		}
		DATA_SAVE(buffer,ptr-buffer);
		free(buffer);
	}
	hallocEnd();
	return True;
}

//	key.data = &block;
//	key.size = sizeof(block);
	
//	ret = db->get(db,NULL,&key,&val,0);
//	if( ret != 0 ) {
//		db = hash_dirty;
//		ret = db->get(db,NULL,&key,&val,0);
//		if( ret != 0) {
//			if(freeq_next == freeq) {
//				syslog(LOG_ALERT,"Used all available cache, please implement FLUSH!");
//				return False;
//			}
//			entry = &new_entry;
//			entry->slot 	= *--freeq_next;
//			entry->block 	= block;
//			entry->usecount	= 0;
//			val.data = entry;
//			val.size = sizeof(hash_entry);		
//		}
//	}
//	if(!entry) entry = (hash_entry*)val.data;
//	entry->usecount++;
//	entry->dirty = DIRTY;

//	if( db != hash_dirty ) {
//		db->del(db,NULL,&key,0);
//	}
//	if( hash_dirty->put(hash_dirty,NULL,&key,&val,0) !=0) {
//		syslog(LOG_ALERT,"Unable to insert entry into HASH");
//		return False;
//	}
	//
//	index->block = block;
//	index->dirty = DIRTY;
//	index->usecount = entry->usecount;
//	memcpy(buffer+sizeof(cache_entry),data,NCACHE_BSIZE);
	//
	//uint64_t offset = data_offset + entry->slot*NCACHE_ESIZE;
	//if(lseek(cache,offset,SEEK_SET)==-1) {		\
	//	syslog(LOG_ALERT,"Seek error, slot=%d,err=%d",(int)entry->slot,errno);	\
	//	return False;													\
	//}
	//WRITE_BLOCK(entry->slot,buffer);
//	return True;
//}

		//SEEK_BLOCK(slot,"WRITE");
		//WRITE_BLOCKS(data,count*NCACHE_BSIZE);
		//len -= count * NCACHE_BSIZE;
		//block = off/NCACHE_BSIZE;
		//key.data = &block;
		//key.size = sizeof(block);
		//if( ret == 0) {		// block exists!
		//	// add block to trim list
		//	if( hash_dirty->put(hash_dirty,NULL,&key,&val,0) !=0) {
		//		syslog(LOG_ALERT,"Unable to insert entry into HASH");
		//		return False;
		//	}
	

///////////////////////////////////////////////////////////////////////////////
//
//	cacheList	- list the contents of the cache
//
///////////////////////////////////////////////////////////////////////////////

int cacheList()
{
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
	syslog(LOG_INFO,"CACHE LISTING");
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
	
	DATA_SEEK(0,"REBUILD");

	printf("Rebuilding Index for Cache Device (%lldM)\n",(unsigned long long)(cache_device.size/1024/1024));
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
		hash_entry entry;
		entry.slot 	= slot;
		entry.block 	= index->block;
		entry.dirty 	= index->dirty;
		entry.usecount	= index->usecount;
			
		val.data = &entry;
		val.size = sizeof(hash_entry);
		key.data = &entry.block;
		key.size = sizeof(entry.block);
			
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
		free(val.data);
	}
	cursor->c_close(cursor);
	if( !units && !ret) return True;
	syslog(LOG_ALERT,"Only able to expire %d blocks (of %d)",count,units);
	return False;
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheTest	- run a basic read-write test in the cache (destructive!)
//
///////////////////////////////////////////////////////////////////////////////

void cacheTest()
{
	int fd = open("/etc/sensors3.conf",O_RDONLY);
	if(fd<1) {
		printf("Error opening file, err=%d\n",errno);
		return;
	}
	
	char buffer[1024*1024];
	int off,len,bytes,bytes2;
	
	off=NCACHE_BSIZE*1000;
	len=NCACHE_BSIZE*4;
	
	memset(buffer,0,sizeof(buffer));
	bytes = read(fd,buffer,sizeof(buffer));
	cacheWrite(off,bytes,&buffer);
	
	void* data;
	off=NCACHE_BSIZE*1000;
	len=bytes;

	while( len > 0) {
		data = cacheRead(off,NCACHE_BSIZE);
		printf("%s",(char*)data);
		len -= NCACHE_BSIZE;
		off += NCACHE_BSIZE;
	}
}

///////////////////////////////////////////////////////////////////////////////
//
//	cacheStats	- print out some stats showing the state of the structures
//
///////////////////////////////////////////////////////////////////////////////

void cacheStats()
{
	void hash_stats(DB* db,char* title)
	{
		void *sp;
		if(db->stat(db,NULL,&sp,0)!=0) {
			printf("STAT Failed");
			return;
		}
		DB_HASH_STAT* stats = (DB_HASH_STAT*)sp;
		printf("------ %s -------\n",title);
		printf("Hash Keys ......... %ld\n",(unsigned long)stats->hash_nkeys);
		printf("Hash Data ......... %ld\n",(unsigned long)stats->hash_ndata);
		printf("Hash Page Count ... %ld\n",(unsigned long)stats->hash_pagecnt);
		printf("Hash Page Size .... %ld\n",(unsigned long)stats->hash_pagesize);
		printf("Hash Fill Factor .. %ld\n",(unsigned long)stats->hash_ffactor);
		printf("Hash Buckets ...... %ld\n",(unsigned long)stats->hash_buckets);
		printf("Hash Free ......... %ld\n",(unsigned long)stats->hash_free);
		printf("Hash Free Bytes ... %ld\n",(unsigned long)stats->hash_bfree);
		printf("Hash Big Pages .... %ld\n",(unsigned long)stats->hash_bigpages);
		printf("Hash Big Free ..... %ld\n",(unsigned long)stats->hash_big_bfree);
		printf("Hash Overflows .... %ld\n",(unsigned long)stats->hash_overflows);
		printf("Hash Oflow free ... %ld\n",(unsigned long)stats->hash_ovfl_free);
		printf("Hash Dup .......... %ld\n",(unsigned long)stats->hash_dup);
		printf("Hash Dup Free ..... %ld\n",(unsigned long)stats->hash_dup_free);
	}
	
	void btree_stats(DB* db,char* title)
	{
		void *sp;
		if(db->stat(db,NULL,&sp,0)!=0) {
			printf("STAT Failed");
			return;
		}
		DB_BTREE_STAT* stats = (DB_BTREE_STAT*)sp;
		printf("------ %s -------\n",title);
		printf("Hash Keys ......... %ld\n",(unsigned long)stats->bt_nkeys);
		printf("Hash Data ......... %ld\n",(unsigned long)stats->bt_ndata);
		printf("Hash Page Count ... %ld\n",(unsigned long)stats->bt_pagecnt);
		printf("Hash Page Size .... %ld\n",(unsigned long)stats->bt_pagesize);
		printf("Min Key ........... %ld\n",(unsigned long)stats->bt_minkey);
		printf("Tree Levels ....... %ld\n",(unsigned long)stats->bt_levels);
		printf("Internal Pages .... %ld\n",(unsigned long)stats->bt_int_pg);
		printf("Leaf Pages ........ %ld\n",(unsigned long)stats->bt_leaf_pg);
		printf("Duplicate Pages ... %ld\n",(unsigned long)stats->bt_dup_pg);
		printf("Overflow Pages..... %ld\n",(unsigned long)stats->bt_over_pg);
		printf("Empty Pages ....... %ld\n",(unsigned long)stats->bt_empty_pg);
		printf("Pages on F/List ... %ld\n",(unsigned long)stats->bt_free);
	}
	
	hash_stats(hash_used,"USED");
	hash_stats(hash_dirty,"DIRTY");
	btree_stats(hash_index,"- LFU -");

}