/* */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define NBD_SET_SOCK    _IO( 0xab, 0 )
#define NBD_SET_BLKSIZE _IO( 0xab, 1 )
#define NBD_SET_SIZE    _IO( 0xab, 2 )
#define NBD_DO_IT       _IO( 0xab, 3 )
#define NBD_CLEAR_SOCK  _IO( 0xab, 4 )
#define NBD_CLEAR_QUE   _IO( 0xab, 5 )
#define NBD_PRINT_DEBUG _IO( 0xab, 6 )
#define NBD_SET_SIZE_BLOCKS     _IO( 0xab, 7 )
#define NBD_DISCONNECT  _IO( 0xab, 8 )
#define NBD_SET_TIMEOUT _IO( 0xab, 9 )
#define NBD_SET_FLAGS _IO( 0xab, 10 )

/* values for flags field */
#define NBD_FLAG_HAS_FLAGS      (1 << 0)        /* Flags are there */
#define NBD_FLAG_READ_ONLY      (1 << 1)        /* Device is read-only */
#define NBD_FLAG_SEND_FLUSH     (1 << 2)        /* Send FLUSH */
#define NBD_FLAG_SEND_FUA       (1 << 3)        /* Send FUA (Force Unit Access) */
#define NBD_FLAG_ROTATIONAL     (1 << 4)        /* Use elevator algorithm - rotational media */
#define NBD_FLAG_SEND_TRIM      (1 << 5)        /* Send TRIM (discard) */

//	Network to Host Long (Long)

#define htonll ntohll

#define NBD_FLAG_FIXED_NEWSTYLE (1 << 0)

//	Constants used for Client-Server negotiation

#define INIT_PASSWD 	    "NBDMAGIC"
#define CACHE_MAGIC			"NBDCACHE"
#define OPTS_MAGIC          0x49484156454F5054LL
#define CLISERV_MAGIC       0x00420281861253LL
#define NBD_REQUEST_MAGIC   0x25609513
#define NBD_REPLY_MAGIC     0x67446698
#define NBD_CLIENT_TIMEOUT  10
#define NBD_PORT	    "10809"
#define NBD_SERVER_PORT	    "10810"
#define NBD_SERVER_ADDR	    "0.0.0.0"

//	Commands we will accept from the client

#define NBD_OPT_EXPORT_NAME 1
#define NBD_OPT_ABORT       2
#define NBD_OPT_LIST        3

#define NBDC_DO_LIST 1

//	Replies we might send back to the client

#define NBD_CMD_MASK_COMMAND    0x0000ffff
#define NBD_REP_ACK		(1) 	                    /** ACK a request. Data: option number to be acked */
#define NBD_REP_SERVER	        (2)	                    /** Reply to NBD_OPT_LIST (one of these per server; must be followed by NBD_REP_ACK to signal the end of the list */
#define NBD_REP_FLAG_ERROR	(1 << 31)	            /** If the high bit is set, the reply is an error */
#define NBD_REP_ERR_UNSUP	(1 | NBD_REP_FLAG_ERROR)    /** Client requested an option not understood by this version of the server */
#define NBD_REP_ERR_POLICY	(2 | NBD_REP_FLAG_ERROR)    /** Client requested an option not allowed by server configuration. (e.g., the option was disabled) */
#define NBD_REP_ERR_INVALID	(3 | NBD_REP_FLAG_ERROR)    /** Client issued an invalid request */
#define NBD_REP_ERR_PLATFORM	(4 | NBD_REP_FLAG_ERROR)	

//	Types of data transaction we will process

enum {
	NBD_READ 	= 0,
	NBD_WRITE 	= 1,
	NBD_CLOSE 	= 2,
	NBD_FLUSH 	= 3,
	NBD_TRIM 	= 4
};

//	Our Local Constants

#define True 1
#define False 0

//	Local Structures we use during communication

struct nbd_request {
        uint32_t    magic;
        uint32_t    type;
        char        handle[8];
        uint64_t    from;
        uint32_t    len;
} __attribute__ ((packed));

struct nbd_reply {
        uint32_t magic;
        uint32_t error;           /* 0 = ok, else error   */
        char handle[8];         /* handle you got from request  */
} __attribute__ ((packed));

typedef struct process {
    int nbd;
    int pid;
    int dev;
    uint64_t siz;
} process;

typedef struct hash_entry {
	uint64_t	block;
	uint32_t	slot;
	uint32_t	usecount;
	uint8_t		dirty;
} hash_entry;

typedef struct cache_entry {
	
	uint64_t	block;
	uint32_t	usecount;	
	uint8_t		dirty;
	
} __attribute__ ((packed)) cache_entry;

typedef struct cache_header {

	char			magic[8];
	uint64_t		size;
	uint8_t			hcount;
	struct in_addr	hosts[6];
	uint8_t			open;
	uint8_t			reindex;
		
} __attribute__ ((packed)) cache_header;

struct thread_info {    /* Used as argument to thread_start() */
           pthread_t thread_id;        /* ID returned by pthread_create() */
           int       thread_num;       /* Application-defined thread # */
           char*	host;
		   char*	name;
       };


#define NCACHE_HSIZE 512
#define NCACHE_CSIZE 32768
#define NCACHE_BSIZE 4096
#define NCACHE_ESIZE (NCACHE_BSIZE + sizeof(cache_entry))
#define CACHE_FACTOR 0.02


#define	DATA_SEEK(slot,label)																				\
	cache_tmp = data_offset + (slot*NCACHE_ESIZE);															\
	lseek(cache,cache_tmp,SEEK_SET);

	
	//if(cache_tmp!=cache_ptr) {																				\
	//	if(lseek(cache,cache_tmp,SEEK_SET)==-1) {															\
	//		syslog(LOG_ALERT,"Seek error, slot=%d,err=%d",(int)slot,errno);									\
	//		return False;																					\
	//	}																									\
	//	cache_ptr = cache_tmp;																				\
	//}

#define DATA_SAVE(buffer,len)											\
	//syslog(LOG_INFO,"SAVE :: buffer=%llx,len=%d",(unsigned long long)buffer,(int)(len));			\
	if( write(cache,buffer,len) != len) {								\
		syslog(LOG_ALERT,"Write error, err=%d",errno);					\
		return False;													\
	}																	\
	cache_ptr += len;

#define SAVE_CACHE(db)													\
	if( db->put(db,NULL,&key,&val,0) !=0) {								\
		syslog(LOG_ALERT,"Error saving HASH key");						\
		return False;													\
	}

#define READ_BLOCK(slot,buffer)	\
	syslog(LOG_INFO,"READ :: slot=%d, buffer=%llx,len=%d",slot,(unsigned long long)buffer,4096);			\
	if( read(cache,buffer,NCACHE_ESIZE) != NCACHE_ESIZE ) {				\
		syslog(LOG_ALERT,"Read error, slot=%d,err=%d",(int)slot,errno);	\
		return False;													\
	}																	\
	cache_ptr += NCACHE_ESIZE;

#define READ_HEADER(cache,header)											\
	if(lseek(cache,0,SEEK_SET)==-1) {										\
		syslog(LOG_ALERT,"Read Header, seek failure, err=%d",errno);		\
		return -1;															\
	}																		\
	if( read(cache,&header,sizeof(header)) != sizeof(header)) {				\
		syslog(LOG_ALERT,"Failed to read cache header");					\
		return -1;															\
	} 	

#define WRITE_HEADER(cache,header)											\
		if(lseek(cache,0,SEEK_SET)==-1) {									\
			syslog(LOG_ALERT,"Write Header, seek failure, err=%d",errno);	\
			return -1;														\
		}																	\
		if( write(cache,&header,sizeof(header)) != sizeof(header)) {		\
			syslog(LOG_ALERT,"Failed to write cache header");				\
			return -1;														\
		} 			




/*
#define	SEEK_BLOCK(slot,label)	\
	//syslog(LOG_INFO,"SEEK slot [%d] for [%s] @ %llx",(int)slot,label,(unsigned long long)(cache,data_offset + slot*NCACHE_ESIZE));			\
	if(lseek(cache,data_offset + slot*NCACHE_ESIZE,SEEK_SET)==-1) {		\
		syslog(LOG_ALERT,"Seek error, slot=%d,err=%d",(int)slot,errno);	\
		return False;													\
	}


#define WRITE_BLOCK(slot,buffer)	\
	if( write(cache,buffer,NCACHE_ESIZE) != NCACHE_ESIZE) {				\
		syslog(LOG_ALERT,"Write error, slot=%d,err=%d",(int)slot,errno);\
		return False;													\
	}

#define WRITE_INDEX(slot,buffer)											\
	if( write(cache,buffer,sizeof(cache_entry)) != sizeof(cache_entry)) {	\
		syslog(LOG_ALERT,"Write error, slot=%d,err=%d",(int)slot,errno);	\
		return False;														\
	}
*/

uint64_t ntohll(uint64_t);
int cacheRead(uint64_t,char*,int);
