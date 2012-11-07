#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "nbd.h"

#define MAX_CHUNK 256

typedef struct hallocEntry {
	
	uint32_t start;
	struct hallocEntry *next;
	
} hallocEntry;

hallocEntry *hstore[MAX_CHUNK];
int hstorec[MAX_CHUNK];
uint32_t hstart,hlast,hentries;

void hallocFlush(int i,uint32_t start)
{
	hallocEntry *entry = (hallocEntry*)malloc(sizeof(hallocEntry));
	entry->start = start;
	if(!hstore[i]) {
		entry->next = NULL;
		hstore[i] = entry;
	} else {
		entry->next = hstore[i];
		hstore[i] = entry;
	}
	hstorec[i]++;
}

void hallocAllocate(uint32_t *slot,int *count)
{
	int i = *count;
	hallocEntry *entry;
	
	assert(*count<MAX_CHUNK); // make sure we're not asking too much
	while( !hstorec[i] && (i<MAX_CHUNK)) i++;
	if(i==MAX_CHUNK) {
		while( !hstorec[i] && (i>0)) i--;
	}
	assert(hstorec[i]);	// else were 100% full!
	
	entry = hstore[i];
	hstore[i] = entry->next;
	
	*slot = entry->start;
	*count = i;
	
	if( i != *count ) hallocFlush(i-*count,entry->start + *count;);
	free(entry);
}

void hallocFree(uint32_t slot)
{
	if( (slot != hlast+1) || (hentries == MAX_CHUNK-1) ) {
		if( hentries ) {
			hallocFlush(hentries,hstart);
		}
		hstart = slot; hentries = 0;
	}
	hentries++;
	hlast = slot;
}

void hallocBegin()
{
	hstart = 0; hlast = 0; hentries = 0;
}

void hallocEnd()
{
	hallocFree(-1);
}

void hallocStats()
{
	int i;
	for(i=0;i<MAX_CHUNK;i++) {
		if(hstore[i]) {
			printf("%3d Blocks :: Start @ %8ld , %8d instances\n",
				   i,(unsigned long)hstore[i]->start,hstorec[i]);
		}
	}
}

void hallocLoad(void* base,int count)
{
	int slot, last = 0 , start = 0 ,entries = 0;
	int i; for(i=0;i<MAX_CHUNK;i++) { hstore[i]=NULL; hstorec[i]=0; }
	
	cache_entry *ptr = (cache_entry*)base;
	for(slot=0;slot<count;slot++) {
		if(!ptr->dirty) {
			if( (slot != last+1) || (entries==(MAX_CHUNK-1))) { // new chain
				if( entries ) {
					hallocFlush(entries,start);
				}
				start = slot; entries = 0;
			}
			entries++;
			last = slot;
		}
		ptr++;
	}
	hallocFlush(entries,start);
	hallocStats();
}
