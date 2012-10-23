/*
 *      nbd-server.c
 *      (c) Gareth Bult 2012
 */

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
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

#define BLOCK_SIZE          	1024
#define INIT_PASSWD 		"NBDMAGIC"
#define NBD_FLAG_FIXED_NEWSTYLE (1 << 0)

#define OPTS_MAGIC          0x49484156454F5054LL
#define NBD_REQUEST_MAGIC   0x25609513
#define NBD_REPLY_MAGIC     0x67446698

#define NBD_OPT_EXPORT_NAME 1
#define NBD_OPT_ABORT       2
#define NBD_OPT_LIST        3

#define NBD_READ            0
#define NBD_WRITE           1
#define NBD_CLOSE           2

#define NBD_CMD_MASK_COMMAND    0x0000ffff
#define NBD_REP_ACK		(1) 	                    /** ACK a request. Data: option number to be acked */
#define NBD_REP_SERVER	        (2)	                    /** Reply to NBD_OPT_LIST (one of these per server; must be followed by NBD_REP_ACK to signal the end of the list */
#define NBD_REP_FLAG_ERROR	(1 << 31)	            /** If the high bit is set, the reply is an error */
#define NBD_REP_ERR_UNSUP	(1 | NBD_REP_FLAG_ERROR)    /** Client requested an option not understood by this version of the server */
#define NBD_REP_ERR_POLICY	(2 | NBD_REP_FLAG_ERROR)    /** Client requested an option not allowed by server configuration. (e.g., the option was disabled) */
#define NBD_REP_ERR_INVALID	(3 | NBD_REP_FLAG_ERROR)    /** Client issued an invalid request */
#define NBD_REP_ERR_PLATFORM	(4 | NBD_REP_FLAG_ERROR)	

typedef unsigned int u32;
typedef unsigned long u64;

u64 ntohll(u64 a) {
        u32 lo = a & 0xffffffff;
        u32 hi = a >> 32U;
        lo = ntohl(lo);
        hi = ntohl(hi);
        return ((u64) lo) << 32U | hi;
}
#define htonll ntohll
#define True 1
#define False 0

int db;
uint64_t off;
uint32_t len;
uint32_t cmd;

int getSocket()
{
    char    *address        = "0.0.0.0";
    char    *port           = "10809";
    struct  addrinfo *ai    = NULL;
    struct  addrinfo hints;
    int     sock;

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
    sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if(sock<0) {
        printf("Unable to allocate socket (%d)\n",errno);
        exit(errno);    
    }
    freeaddrinfo(ai);
    //
    if(setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
        printf("Unable to set REUSEADDR on socket (%d)\n",errno);
        exit(errno);
    }
    if(setsockopt(sock,SOL_SOCKET,SO_LINGER,&l,sizeof(l)) == -1) {
        printf("Unable to set LINGER on socket (%d)\n",errno);
        exit(errno);
    }
    if(setsockopt(sock,SOL_SOCKET,SO_KEEPALIVE,&yes,sizeof(int)) == -1) {
        printf("Unable to set KEEPALIVE on socket (%d)\n",errno);
        exit(errno);
    }
    if(bind(sock, ai->ai_addr, ai->ai_addrlen)) {
        if(errno=EADDRINUSE)
                printf("Address is already in use (%d)\n",errno);
        else    printd("Error binding to socket (%d)\n",errno);
        exit(errno);
    }
    if(listen(sock,1)) {
        printf("Error LISTENING on socket (%d)\n",errno);
        exit(errno);
    }
    return sock;
}

void doLog(char *text,int sock)
{
    time_t t = time(NULL);
    char *now = ctime(&t);
    now[strlen(now)-1]=0;
    printf("%s :: %s",now,text);
    if(sock) printf(" (sock=%d)",sock);
    if(errno) printf(" [%d]",errno);
    printf("\n");
}

void getBytes(int sock, void *buf, size_t len)
{
    int bytes;
    int max = len;
    
    //printf("Reading %d\n",(int)len);
    while( len ) {
        bytes = read(sock,buf,len);
        if( bytes <= 0) {
            if( errno != EAGAIN ) {
                doLog("Critical Error in READ",sock);
                exit(errno);
            }
        }
        buf += bytes;
        len -= bytes;
    }
}

void putBytes(int sock, void *buf, size_t len)
{
    //printf("Writing %d\n",(int)len);
    //int i;
    //char *ptr = (char*)buf;
    //for(i=0;i<len;i++) {
    //    printf("%02x ",*ptr++);
   // }
    //printf("\n");
    if( write( sock,buf,len ) < 0) {
        doLog("Critical Error in WRITE",sock);
        exit(errno);
    }
}


void doConnectionMade(int sock)
{
    struct {
        volatile uint64_t passwd __attribute__((packed));
        volatile uint64_t magic  __attribute__((packed));
        volatile uint16_t flags  __attribute__((packed));
    } nbd;
    
    memcpy(&nbd,INIT_PASSWD,8);
    nbd.magic  = htonll(OPTS_MAGIC);
    nbd.flags  = htons(NBD_FLAG_FIXED_NEWSTYLE);
    
    doLog("Connection Made",0);
    putBytes(sock,&nbd,sizeof(nbd));
}

static void send_reply(uint32_t opt, int sock, uint32_t reply_type, size_t datasize, void* data)
{
    uint64_t magic = htonll(0x3e889045565a9LL);
    reply_type = htonl(reply_type);
    uint32_t datsize = htonl(datasize);       
        
    doLog("Reply",sock);
    putBytes(sock,&magic,sizeof(magic));
    putBytes(sock,&opt,sizeof(opt));
    putBytes(sock,&reply_type,sizeof(reply_type));
    putBytes(sock,&datsize,sizeof(datsize));
    if(datasize) putBytes(sock,data,datasize);
}

int doNegotiate(int sock)
{
        struct {
        volatile uint64_t magic __attribute__((packed));
        volatile uint32_t opts  __attribute__((packed));
    } nbd;
    
    int working = True;
    uint32_t flags;
    getBytes(sock,&flags,sizeof(flags));
    flags = htonl(flags);
    
    uint32_t len;
    char *name;
    uint32_t opt;

    while( working ) {
        getBytes(sock,&nbd,sizeof(nbd));
        if(nbd.magic != htonll(OPTS_MAGIC)) {
            doLog("Client Magic is bad",sock);
            return False;
        }
        opt = ntohl(nbd.opts);
        switch( opt )
        {
            case NBD_OPT_EXPORT_NAME:                
                getBytes(sock,&len,sizeof(len));
                len = ntohl(len);
                name = malloc(len+1);
                name[len]=0;
                getBytes(sock,name,len);
                printf("Export name = %s\n",name);
                working = False;                
                break;
            case NBD_OPT_LIST:
                doLog("Received LIST from client",sock);
                getBytes(sock,&len,sizeof(len));
                len=ntohl(len);
                printf("Size=%d\n",len);
                if(len) {
                    send_reply(opt, sock, NBD_REP_ERR_INVALID, 0, NULL);    
                }
                char buf[128];
                char *ptr = (char*)&buf;
                
                len = htonl(4);
                memcpy(ptr,&len,sizeof(len));
                ptr += sizeof(len);
                memcpy(ptr,"demo",4);
		send_reply(opt, sock, NBD_REP_SERVER, 8, buf);
                send_reply(opt, sock, NBD_REP_ACK, 0, NULL);
                break;
            case NBD_OPT_ABORT:
                doLog("Received ABORT from client",sock);
                return False;
            default:
                printf("Default=%d\n", opt );
		break;
        }
    }
    int64_t size = htonll(1024*1024*1024);
    int16_t small = htons(1);
    char zeros[124];
    memset(zeros,0,sizeof(zeros));
    putBytes(sock,&size,sizeof(size));
    putBytes(sock,&small,sizeof(small));
    putBytes(sock,&zeros,sizeof(zeros));
    return True;
}

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
};

int dbError(char *text)
{
	char message[256];
	sprintf("__ERROR on %s :: Offset = %lld Len = %lld [err=%d]",
		text,(unsigned long long)off,(unsigned long long)len,errno);
	return False;
}

void doSession(int sock)
{
    doLog("Running client session",sock);
    doConnectionMade(sock);
    if(doNegotiate(sock)) {
	
        doLog("Session",0);
        
        struct nbd_request request;
        struct nbd_reply reply;
	char buffer[1024*132];
	int readlen,bytes;
	char pbuf[128];
		
	//char *cmds[5];
	//cmds[NBD_READ] ="READ  - ";
	//cmds[NBD_WRITE]="WRITE - ";
	//cmds[NBD_CLOSE]="CLOSE - ";
        
        int running = True;
        
        while( running ) {
            
            getBytes(sock,&request,sizeof(request));
            off = ntohll(request.from);
            cmd = ntohl(request.type) & NBD_CMD_MASK_COMMAND;
            len = ntohl(request.len);
            reply.magic = htonl(NBD_REPLY_MAGIC);
            reply.error = 0;
	    
	    //sprintf(pbuf,"%s block [%04llx] %lld blocks",cmds[cmd],(long long unsigned int)block,(long long unsigned int)blocks);
	    //doLog(pbuf,sock);	    
            //printf("Magic (%d), Req(%d) Off(%ld) Len(%d)\n",request.magic,cmd,off,len);
            
            switch(cmd) {
            
                case NBD_READ:
			memcpy(reply.handle, request.handle, sizeof(reply.handle));
			putBytes(sock,&reply,sizeof(reply));									
			if(lseek(db,off,SEEK_SET)<0) running = dbError("SEEK");
			while( len > 0 ) {
				readlen = len > sizeof(buffer)?sizeof(buffer):len;				
				bytes = read(db,&buffer,readlen);
				if( bytes != readlen ) running = dbError("READ");
				else putBytes(sock,&buffer,readlen);
	                        len -= readlen;
			}
			break;
                
                case NBD_WRITE:
		    
			memcpy(reply.handle, request.handle, sizeof(reply.handle));
			if(lseek(db,off,SEEK_SET)<0) running = dbError("SEEK");
			while( len > 0 ) {
				readlen = len > sizeof(buffer)?sizeof(buffer):len;
	                        getBytes(sock,&buffer,readlen);
				bytes = write(db,&buffer,readlen);
				if( bytes != readlen ) running = dbError("WRITE");
				len -= readlen;
	                }
			putBytes(sock,&reply,sizeof(reply));
			break;
            
                case NBD_CLOSE:
			running = False;
			break;
                
                default:
			printf("Unhandled command=%d\n",cmd);
			doLog("Unknown command",sock);
            }
        }           
    } 
    doLog("Terminating client session",sock);
}

void doAccept(int sock)
{
    fd_set rfds;
    struct sockaddr_storage addrin;
    socklen_t addrinlen=sizeof(addrin);
    int net;

    doLog("Running ACCEPT Loop",0);
    while(1) {
        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);
        if(select(sock+1, &rfds, NULL, NULL, NULL)>0) {
            if(FD_ISSET(sock, &rfds)) {
                if ((net=accept(sock, (struct sockaddr *) &addrin, &addrinlen)) < 0) {
                    doLog("Accept error on socket",net);
                } else {
                    doSession(net);
                    close(net);
                }
            }
        }
        break; // FIXME :: for debug !!
    }
    doLog("Terminating ACCEPT Loop",0);
}
  
void main(int argc,char *argv[])
{
	doLog("Running NBD Server",0);
	
	db = open("/dev/vols/blocks/demo",O_RDWR,O_DIRECT);
	if( db < 0) {
		doLog("Unable to open BLOCK DEVICE",0);
		exit(1);
	}
	int sock = getSocket();    
	doAccept(sock);
	close(sock);
	doLog("Terminating NBD Server",0);
	exit(0); 
}