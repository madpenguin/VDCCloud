/*
 *      nbd-server.c
 *      (c) Gareth Bult 2012
 *
 *	This is a cut-down version of the nbd-server specifically targetted at the VDC Cloud project
 *	written from scratch with the following in mind;
 *		No redundant features
 *		Required volume name obtained dynamically from the client [authentication to come]
 *		Designed to run on ZFS volumes specifically
 *		Designed to record checksum and transaction log information
 *		More compact than original code
 *		For use with Caching / RAID nbd-client
 *
 *	TODO :: Backgrounding
 *     	TODO :: Forking / detaching children
 */

//	Headers / Include Files
 
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <syslog.h>
#include <signal.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <netdb.h>
#include <syslog.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>

/*
 *	Flags to indicate new-style negotiation
 *	TODO :: This is potentially obsolete
 *
 */

#define NBD_FLAG_FIXED_NEWSTYLE (1 << 0)

//	Constants used for Client-Server negotiation

#define INIT_PASSWD 	    "NBDMAGIC"
#define OPTS_MAGIC          0x49484156454F5054LL
#define NBD_REQUEST_MAGIC   0x25609513
#define NBD_REPLY_MAGIC     0x67446698

//	Commands we will accept from the client

#define NBD_OPT_EXPORT_NAME 1
#define NBD_OPT_ABORT       2
#define NBD_OPT_LIST        3

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

#define NBD_READ	0
#define NBD_WRITE	1
#define NBD_CLOSE	2

//	Network to Host Long (Long)

uint64_t ntohll(uint64_t a) {
        uint32_t lo = a & 0xffffffff;
        uint32_t hi = a >> 32U;
        lo = ntohl(lo);
        hi = ntohl(hi);
        return ((uint64_t) lo) << 32U | hi;
}
#define htonll ntohll

//	Our Local Constants

#define BLOCK_SIZE	1024
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

//	Global Variables
//	As we will eventually fork one of these per connection, it's just easier
//	to use global variables than passing file descriptora around.

int 		sock;		// current client connection
int 		db;		// current database connection
uint64_t 	off;		// offset of current transation
uint32_t 	len;		// length of current transaction
uint32_t 	cmd;		// command of current transaction
int 		debug = 0;	// global debug flag
char		pbuf[1024];	// print buffer
char* 		cmds[]	= { "READ" , "WRITE" , "CLOSE" };

void doLog(char *text)
{
	if(debug<2) {
		syslog(LOG_INFO,"%s",text);
		return;
	}	
	time_t t = time(NULL);
	char *now = ctime(&t);
	now[strlen(now)-1]=0;
	printf("%s :nbd: %s\n",now,text);
}

int doError(char *text)
{
	char msg[256];
	snprintf(msg,sizeof(msg),
		 "error:%s socket=%d db=%d off=%lld len=%lld errno=%d",
		 text,sock,db,(unsigned long long)off,(unsigned long long)len,errno);
	if(debug<2)
		syslog(LOG_ERR,"%s",msg);
	else	doLog(msg);	
	return False;
}

//	getSocket - Instantiate a network socket and set up all the trimmings ...

int getSocket()
{
	char    *address        = "0.0.0.0";
	char    *port           = "10809";
	struct  addrinfo *ai    = NULL;
	struct  addrinfo hints;
	int 	s;

	memset(&hints,'\0',sizeof(hints));
	hints.ai_flags      = AI_PASSIVE | AI_ADDRCONFIG | AI_NUMERICSERV;
	hints.ai_socktype   = SOCK_STREAM;
	hints.ai_family     = AF_INET;
	char *yes           = "1";
	struct linger l;
	//
	l.l_onoff = 1;
	l.l_linger = 10;
	//
	if(getaddrinfo(address,port,&hints,&ai)) {
	    printf("Unable to get address info (%d)\n",errno);
	    exit(errno);
	}
	s = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
	if(s<0) {
	    printf("Unable to allocate socket (%d)\n",errno);
	    exit(errno);    
	}
	freeaddrinfo(ai);
	//
	if(setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
	    printf("Unable to set REUSEADDR on socket (%d)\n",errno);
	    exit(errno);
	}
	if(setsockopt(s,SOL_SOCKET,SO_LINGER,&l,sizeof(l)) == -1) {
	    printf("Unable to set LINGER on socket (%d)\n",errno);
	    exit(errno);
	}
	if(setsockopt(s,SOL_SOCKET,SO_KEEPALIVE,&yes,sizeof(int)) == -1) {
	    printf("Unable to set KEEPALIVE on socket (%d)\n",errno);
	    exit(errno);
	}
	if(bind(s, ai->ai_addr, ai->ai_addrlen)) {
	    if(errno=EADDRINUSE)
	            printf("Address is already in use (%d)\n",errno);
	    else    printd("Error binding to socket (%d)\n",errno);
	    exit(errno);
	}
	if(listen(s,1)) {
	    printf("Error LISTENING on socket (%d)\n",errno);
	    exit(errno);
	}
	return s;
}

//	getBytes - get data from the client (via Network)

void getBytes(void *buf, size_t len)
{
	int bytes;
	int max = len;
	char msg[1024];	
	
	if(debug>1) {
		sprintf(msg,"getBytes=%d",(int)len);
		doLog(msg);
	}    
	while( len ) {
		bytes = read(sock,buf,len);
		if( bytes <= 0) {
			if( errno != EAGAIN ) {
			doLog("Critical Error in READ");
			exit(errno);
			}
		} else {
			if(debug>2) {
				int i;	
				char *ptr = (char*)buf;
				for(i=0;i<bytes;i++) {
					if(i>200) break;
					printf("%02x ",*ptr++ && 255);
				}
				printf("\n");
			}			
			buf += bytes;
			len -= bytes;
		}
	}
}

//	putBytes - Send information to the client (via Network)

void putBytes(void *buf, size_t len)
{
	char msg[1024];
	
	if(debug>1) {
		sprintf(msg,"putBytes=%d",(int)len);
		doLog(msg);
	}
	if(debug>2) {
		int i;		
		char *ptr = (char*)buf;
		for(i=0;i<len;i++) {
			if(i>200) break;
			printf("%02x ",*ptr++ && 255);
		}
		printf("\n");
	}
	if( write( sock,buf,len ) < 0) {
		doLog("Critical Error in WRITE");
		exit(errno);
	}
}

//	doConnectionMade - hangle a new incoming connection

void doConnectionMade()
{
	struct {
		volatile uint64_t passwd __attribute__((packed));
		volatile uint64_t magic  __attribute__((packed));
		volatile uint16_t flags  __attribute__((packed));
	} nbd;
    
	memcpy(&nbd,INIT_PASSWD,8);
	nbd.magic  = htonll(OPTS_MAGIC);
	nbd.flags  = htons(NBD_FLAG_FIXED_NEWSTYLE);
    
	doLog("Connection Made");
	putBytes(&nbd,sizeof(nbd));
}

//	sendReply - send a reply with a pre-determined format to the client

static void sendReply(uint32_t opt,uint32_t reply_type, size_t datasize, void* data)
{
	uint64_t magic = htonll(0x3e889045565a9LL);
	reply_type = htonl(reply_type);
	uint32_t datsize = htonl(datasize);       
        
	doLog("sendReply");
	putBytes(&magic,sizeof(magic));
	putBytes(&opt,sizeof(opt));
	putBytes(&reply_type,sizeof(reply_type));
	putBytes(&datsize,sizeof(datsize));
	if(datasize) putBytes(data,datasize);
}

//	doNegotiate - negotiate a new connection with the client

int doNegotiate()
{
	struct {
	        volatile uint64_t magic __attribute__((packed));
	        volatile uint32_t opts  __attribute__((packed));
	} nbd;
    
	int working = True;
	uint32_t flags;
	getBytes(&flags,sizeof(flags));
	flags = htonl(flags);
	    
	uint32_t len;
	char *name;
	uint32_t opt;
	   
	char path[256];
	int status = False;

	doLog("Enter NEGOTIATION");
	
	do {
		getBytes(&nbd,sizeof(nbd));
		if(nbd.magic != htonll(OPTS_MAGIC)) {
			doError("Bad MAGIC from Client");
			status = False;
			working = False;	
			break;
		}
		opt = ntohl(nbd.opts);
		switch( opt )
		{
			case NBD_OPT_EXPORT_NAME:                
				getBytes(&len,sizeof(len));
				len = ntohl(len);
				name = malloc(len+1);
				name[len]=0;
				getBytes(name,len);
		
				sprintf(path,"/dev/vols/blocks/%s",name);
				db = open(path,O_RDWR,O_DIRECT);
				if( db < 0) {
					doError("Unable to open BLOCK DEVICE");
					doError(path);
					status = False;
					working = False;
					break;
				}
				working = False;
				status = True;
				free(name);
				break;
	
			case NBD_OPT_LIST:
				doLog("Received LIST from client");
				getBytes(&len,sizeof(len));
				len=ntohl(len);
				if(len) sendReply(opt, NBD_REP_ERR_INVALID, 0, NULL);    
                
				char buf[128];
				char *ptr = (char*)&buf;
                
				len = htonl(4);
				memcpy(ptr,&len,sizeof(len));
				ptr += sizeof(len);
				memcpy(ptr,"demo",4);
				sendReply(opt, NBD_REP_SERVER, 8, buf);
				sendReply(opt, NBD_REP_ACK, 0, NULL);
				break;
	
			case NBD_OPT_ABORT:
				doLog("Received ABORT from client");
				status = False;
				working = False;
				break;
	
			default:
				doError("Unknown command");
				break;
		}
	} while( working );
	
	if(!status) return False;
	int64_t size = htonll(1024*1024*1024);
	int16_t small = htons(1);
	char zeros[124];
	memset(zeros,0,sizeof(zeros));
	putBytes(&size,sizeof(size));
	putBytes(&small,sizeof(small));
	putBytes(&zeros,sizeof(zeros));
	doLog("Exit NEGOTIATION [Ok]");
	return True;
}

//	doSession - process a single client session

void doSession()
{
	struct nbd_request request;
	struct nbd_reply reply;
	char buffer[1024*132];
	int readlen,bytes;
	int running = True;

	doLog("Enter SESSION");
	doConnectionMade();
	if(doNegotiate()) {
	
		doLog("Processing DATA requests ...");
        		        
		do {
            
			getBytes(&request,sizeof(request));
			off = ntohll(request.from);
			cmd = ntohl(request.type) & NBD_CMD_MASK_COMMAND;
			len = ntohl(request.len);
			reply.magic = htonl(NBD_REPLY_MAGIC);
			reply.error = 0;
			if(debug) {	
				snprintf(pbuf,sizeof(pbuf),"%s - block [%04llx] %ld blocks, off=%lld, len=%ld",	
					cmds[cmd],						
					(long long unsigned int)(off/BLOCK_SIZE),		
					(long unsigned int)(len/BLOCK_SIZE),			
					(long long unsigned int)off,(long unsigned int)len	
				);								
				doLog(pbuf);							
			}	
			switch(cmd) {
            
				case NBD_READ:
					memcpy(reply.handle, request.handle, sizeof(reply.handle));
					putBytes(&reply,sizeof(reply));									
					if(lseek(db,off,SEEK_SET)<0) running = doError("SEEK");
					while( len > 0 ) {
						readlen = len > sizeof(buffer)?sizeof(buffer):len;				
						bytes = read(db,&buffer,readlen);
						if( bytes != readlen ) running = doError("READ");
						else putBytes(&buffer,readlen);
						len -= readlen;
					}
				break;
                
			case NBD_WRITE:
		    
				memcpy(reply.handle, request.handle, sizeof(reply.handle));
				if(lseek(db,off,SEEK_SET)<0) running = doError("SEEK");
				while( len > 0 ) {
					readlen = len > sizeof(buffer)?sizeof(buffer):len;
				        getBytes(&buffer,readlen);
					bytes = write(db,&buffer,readlen);
					if( bytes != readlen ) running = doError("WRITE");
					len -= readlen;
				}
				putBytes(&reply,sizeof(reply));
				break;
            
			case NBD_CLOSE:
				running = False;
				break;
                
			default:
				doError("Unknown Command");
			}	
		} while( running );          
	}
	if(db) close(db);
	doLog("Exit SESSION");
}

//	doAccept - accept loop for new connections

void doAccept(int listener)
{
	int net;
	fd_set rfds;
	struct sockaddr_storage addrin;
	socklen_t addrinlen=sizeof(addrin);

	doLog("Enter ACCEPT");
	while(1) {
		FD_ZERO(&rfds);
		FD_SET(listener, &rfds);
		if(select(listener+1, &rfds, NULL, NULL, NULL)>0) {
			if(FD_ISSET(listener, &rfds)) {
				if ((sock=accept(listener, (struct sockaddr *) &addrin, &addrinlen)) < 0) {
					doError("Error on ACCEPT");
					continue;
				} 
				db = -1;
				doSession();
				close(sock);
			}
		}	
	}
	doLog("Exit ACCEPT");
}

//	main - just what it says on the can ...
  
void main(int argc,char **argv)
{
	int listener;
	int c;
	
	while ((c = getopt (argc, argv, "d")) != -1)
	{
		switch(c)
		{
			case 'd':
				debug++;
				break;
			default:
				exit(1);
		}
	}
        openlog ("nbd", LOG_CONS|LOG_PID|LOG_NDELAY , LOG_USER);
	doLog("NBD server v0.1 started");
	if((listener=getSocket())>0) {
		doAccept(listener);
		close(listener);
	}
	doLog("NBD server stopped");
	closelog ();
	exit(0); 
}
