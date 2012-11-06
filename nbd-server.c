/*
 *      nbd-server.c
 *      (c) Gareth Bult 2012
 *
 *	This is a cut-down version of the nbd-server specifically targetted at the VDC Cloud project
 *	written from scratch with the following in mind;
 *	
 *		No redundant features
 *		Required volume name obtained dynamically from the client [authentication to come]
 *		Designed to run on ZFS volumes specifically
 *		Designed to record checksum and transaction log information
 *		More compact than original code
 *		For use with Caching / RAID nbd-client
 *
 *     	TODO :: Record volume name for posterity
 *     	TODO :: Implement "list" option to present real data
 *     	TODO :: Integrate Mongo config
 *     	TODO :: Localise socket for multiple connections
 *     	TODO :: Re-Add O_DIRECT but use posix_memalign to allocate buffer space
 *	
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
#include <linux/fs.h>
#include <fcntl.h>
#include "nbd.h"

#define PIDFILE "/var/run/nbd-server.pid"
#define BUF_SIZE 100            		

/*
 *	Flags to indicate new-style negotiation
 *      TODO :: This is potentially obsolete
 *      TODO :: remove device listing support
 *
 */

//	Global Variables
//	As we will eventually fork one of these per connection, it's just easier
//	to use global variables than passing file descriptora around.

uint64_t 	off;		// offset of current transation
uint32_t 	len;		// length of current transaction
uint32_t 	cmd;		// command of current transaction
int 		debug = 0;	// global debug flag
char		pbuf[1024];	// print buffer
char* 		cmds[]	= { "READ" , "WRITE" , "CLOSE" , "FLUSH" , "TRIM" };

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
		 "error:%s off=%lld len=%lld errno=%d",
		 text,(unsigned long long)off,(unsigned long long)len,errno);
	if(debug<2)
		syslog(LOG_ERR,"%s",msg);
	else	doLog(msg);	
	return False;
}

//	getSocket - Instantiate a network socket and set up all the trimmings ...

int getSocket()
{
	char    *address        = NBD_SERVER_ADDR;
	char    *port           = NBD_SERVER_PORT;
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

void getBytes(int sock,void *buf, size_t len)
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

void putBytes(int sock,void *buf, size_t len)
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
    
	doLog("Connection Made");
	putBytes(sock,&nbd,sizeof(nbd));
}

//	sendReply - send a reply with a pre-determined format to the client

static void sendReply(int sock,uint32_t opt,uint32_t reply_type, size_t datasize, void* data)
{
	uint64_t magic = htonll(0x3e889045565a9LL);
	reply_type = htonl(reply_type);
	uint32_t datsize = htonl(datasize);       
        
	doLog("sendReply");
	putBytes(sock,&magic,sizeof(magic));
	putBytes(sock,&opt,sizeof(opt));
	putBytes(sock,&reply_type,sizeof(reply_type));
	putBytes(sock,&datsize,sizeof(datsize));
	if(datasize) putBytes(sock,data,datasize);
}

//	doNegotiate - negotiate a new connection with the client

int doNegotiate(int sock)
{
    struct {
	        volatile uint64_t magic __attribute__((packed));
	        volatile uint32_t opts  __attribute__((packed));
	} nbd;
    
	uint32_t flags;    
	uint32_t len;
	uint32_t opt;
	
	char 	*name;	
	char 	path[256];
	int 	status = False;
	int 	working = True;
	int	db;

	getBytes(sock,&flags,sizeof(flags));
	flags = htonl(flags);
	
	doLog("Enter NEGOTIATION");
	
	do {
		getBytes(sock,&nbd,sizeof(nbd));
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
				getBytes(sock,&len,sizeof(len));
				len = ntohl(len);
				name = malloc(len+1);
				name[len]=0;
				getBytes(sock,name,len);
				syslog(LOG_INFO,"Incoming name = %s",name);
		
				sprintf(path,"/dev/vols/blocks/%s",name);
				db = open(path,O_RDWR|O_EXCL);
				if( db == -1) {
					sprintf(path,"/dev/vols/blocks/%s1",name);
					db = open(path,O_RDWR|O_EXCL);
					if( db ==-1) {
						doError("Unable to open BLOCK DEVICE");
						doError(path);
						status = False;
						working = False;
						break;
					}
				}
				if(db != -1) { syslog(LOG_INFO,"Opened [%s] with descriptor [%d]",path,db); }
				working = False;
				status = True;
				free(name);
				break;
	
			/*case NBD_OPT_LIST:
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
				break; */
	
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
	
	
	int64_t size = 0;
	ioctl(db, BLKGETSIZE, &size);
	syslog(LOG_INFO,"Device Size = %lld\n",(unsigned long long)size);
	size = htonll(size*512);
	int16_t small = htons(1);
	char zeros[124];
	memset(zeros,0,sizeof(zeros));
	putBytes(sock,&size,sizeof(size));
	putBytes(sock,&small,sizeof(small));
	putBytes(sock,&zeros,sizeof(zeros));
	doLog("Exit NEGOTIATION [Ok]");
	return db;
}

//	doSession - process a single client session

void doSession(int sock)
{
	struct nbd_request request;
	struct nbd_reply reply;
	char buffer[1024*132];
	int readlen,bytes;
	int running = True;
	int db;

	doLog("Enter SESSION");
	doConnectionMade(sock);
	if(db=doNegotiate(sock)) {
	
		doLog("Processing DATA requests ...");
        		        
		do {
            
			getBytes(sock,&request,sizeof(request));
			off = ntohll(request.from);
			cmd = ntohl(request.type) & NBD_CMD_MASK_COMMAND;
			len = ntohl(request.len);
			reply.magic = htonl(NBD_REPLY_MAGIC);
			reply.error = 0;
			memcpy(reply.handle, request.handle, sizeof(reply.handle));
			
			if(debug) {	
				snprintf(pbuf,sizeof(pbuf),"%s - block [%04llx] %ld blocks, off=%lld, len=%ld",	
					cmds[cmd],						
					(long long unsigned int)(off/1024),		
					(long unsigned int)(len/1024),			
					(long long unsigned int)off,(long unsigned int)len	
				);								
				doLog(pbuf);							
			}	
			switch(cmd) {
				case NBD_READ:
					putBytes(sock,&reply,sizeof(reply));									
					if(lseek(db,off,SEEK_SET)==-1) running = doError("SEEK");
					while( len > 0 ) {
						readlen = len > sizeof(buffer)?sizeof(buffer):len;
						//syslog(LOG_ERR,"READ: %d, %lld %ld",db,(unsigned long long)off,(unsigned long) len);
						bytes = read(db,&buffer,readlen);
						if( bytes != readlen ) running = doError("READ");
						else putBytes(sock,&buffer,readlen);
						len -= readlen;
					}
				break;
                
			case NBD_WRITE:
				if(lseek(db,off,SEEK_SET)==-1) running = doError("SEEK");
				while( len > 0 ) {
					readlen = len > sizeof(buffer)?sizeof(buffer):len;
				        getBytes(sock,&buffer,readlen);
					bytes = write(db,&buffer,readlen);
					if( bytes != readlen ) running = doError("WRITE");
					len -= readlen;
				}
				putBytes(sock,&reply,sizeof(reply));
				break;
            
			case NBD_CLOSE:
				running = False;
				break;
			
			case NBD_TRIM:
				// FIXME :: TRIM Code needed
				putBytes(sock,&reply,sizeof(reply));
				break;
				
			case NBD_FLUSH:
				// FIXME :: FLUSH Code needed
				putBytes(sock,&reply,sizeof(reply));
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
	int net,f,sock;
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
				f = fork();
				if(f<0) { printf("Fork error [err=%d]\n",errno); exit(1); }
				if(f==0) {
					syslog(LOG_INFO,"Forkd new process with id = %d",getpid());
					setsid();
					for (f=getdtablesize();f>=0;--f) if(f!=sock) close(f);
					f=open("/dev/null",O_RDWR); dup(f); dup(f); umask(027); chdir("/tmp/");
					doSession(sock);
					close(sock);
					syslog(LOG_INFO,"Process with pid %d terminated",getpid());
					exit(0);
				}
				close(sock);
			}
		}	
	}
	doLog("Exit ACCEPT");
}

//	doCreatePid - process id file management

int doCreatePid()
{
	int fd, pid, flags = 0;
	char buf[BUF_SIZE];
	
createpid:

	fd = open(PIDFILE, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (fd == -1) {
		doLog("Failed to open PIDFILE");
		return -1;
	}
        flags = fcntl(fd, F_GETFD);
        if (flags == -1) {
		doLog("Failed to get PIDFILE flags");
		return -1;
	}
        flags |= FD_CLOEXEC;       
        if (fcntl(fd, F_SETFD, flags) == -1) {
		doLog("Failed to set CLOEXEC on pidfile");
		return -1;
	}
	if (lockf(fd, F_TLOCK , 0) == -1) {
		if (errno  == EAGAIN || errno == EACCES) {
			read(fd,&buf,sizeof(buf));
			close(fd);
			pid = atoi(buf);
			syslog(LOG_ERR,"Attempting to kill pid=%d",pid);
			if(pid>1) kill(pid,SIGTERM);
			sleep(1);
			goto createpid;
		}
	        else {
			doLog("Failed to LOCK PID file");
			return -1;
		}
	}
	if (ftruncate(fd, 0) == -1) {
		doLog("Failed to truncate PIDFILE");
		return -1;
	}
	snprintf(buf, BUF_SIZE, "%ld\n", (long) getpid());
	if (write(fd, buf, strlen(buf)) != strlen(buf)) {
		doLog("Failed to write to PIDFILE");
		return -1;
	}
	return fd;
}
	
//	main - just what it says on the can ...
  
void main(int argc,char **argv)
{
	int listener;
	int c;
	int f;
	
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
	if(!debug) {
		f = fork();
		if(f<0) { printf("Fork error [err=%d]\n",errno); exit(1); }
		if(f>0) exit(0); // Parent
		setsid(); for (f=getdtablesize();f>=0;--f) close(f);
		f=open("/dev/null",O_RDWR); dup(f); dup(f); umask(027); chdir("/tmp/");
		signal( SIGCHLD, SIG_IGN ); 
	}
        openlog ("nbd", LOG_CONS|LOG_PID|LOG_NDELAY , LOG_USER);
	doLog("NBD server v0.1 started");
	if(doCreatePid()<0) {
		doLog("-- ABORT");
		exit(1);
	}
	if((listener=getSocket())>0) {
		doAccept(listener);
		close(listener);
	}
	doLog("NBD server stopped");
	closelog ();
	exit(0); 
}
