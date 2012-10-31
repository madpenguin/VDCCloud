/*
 *      nbd-client.c
 *      (c) Gareth Bult 2012
 *
 *	Specifically this client supports two connections at the same time with
 *	a view to providing links for network RAID10. It also provides scope for
 *	more than two links, and local caching / RAID rebuilds etc ...
 *
 *	TODO :: improve NBD allocation / remove sleep timer
 *	TODO :: incorporate caching mechanism
 *	TODO :: incorporate RAID / recovory options
 *  TODO :: remove device listing support
 * 	TODO :: Record volume name for posterity
 * 	TODO :: Convert READ/WRITE to use AIO for proper concurrency
 * 	TODO :: Move to shared memory model
 */

//	Headers / Include Files
 
#include <unistd.h>
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
#include <signal.h>
#include "nbd.h"

int             debug;
extern char*    optarg;
int             running = True;
int 			db;		// current database connection
uint64_t 		off;		// offset of current transation
uint32_t 		len;		// length of current transaction
uint32_t 		cmd;		// command of current transaction
int 			debug = 0;	// global debug flag
char			pbuf[1024];	// print buffer
char* 			cmds[]	= { "READ" , "WRITE" , "CLOSE" , "FLUSH" , "TRIM" };
char            path1[64],path2[64];
int             fd1,fd2;
char            *host1=NULL,*host2=NULL;
process         *p1,*p2;
process         procs[8];
int             pcount=0;

//  doKill - make sure the kids are dead ..

void doKill()
{
    int i;
    for(i=0;i<pcount;i++)
    {
	syslog(LOG_INFO,"Terminating process # %d with pid %d",i,procs[i].pid);
	ioctl(procs[i].nbd, NBD_CLEAR_QUE);
	ioctl(procs[i].nbd, NBD_DISCONNECT);
	ioctl(procs[i].nbd, NBD_CLEAR_SOCK);
	kill(procs[i].pid,SIGINT);	
    }
}

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
		 "error:%s db=%d off=%lld len=%lld errno=%d",
		 text,db,(unsigned long long)off,(unsigned long long)len,errno);
	if(debug<2)
		syslog(LOG_ERR,"%s",msg);
	else	doLog(msg);	
	return False;
}

int getNextFreeDev()
{
    char path[256];
    int index = 0;

    while( index < 256 )
    {
	sprintf(path,"/sys/devices/virtual/block/nbd%d/pid",index);
	if( access( path, F_OK) == -1 ) {
	    return index;
	}
	index++;
    }
    return -1;
}

int doConnect(char* host)
{   
    struct addrinfo hints;
    struct addrinfo *ai = NULL;
    struct addrinfo *rp = NULL;
    int e;
    int s;    
    int size = 1;

    memset(&hints,'\0',sizeof(hints));
    hints.ai_family     = AF_UNSPEC;
    hints.ai_socktype   = SOCK_STREAM;
    hints.ai_flags      = AI_ADDRCONFIG | AI_NUMERICSERV;
    hints.ai_protocol   = IPPROTO_TCP;
    
    if( getaddrinfo(host, NBD_SERVER_PORT, &hints, &ai) < 0) {
        printf("Unable to resolve hostname, errno=%d\n",errno);
        exit(1);
    }
    s = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if(s<0) {
        printf("Unable to allocate socket (%d)\n",errno);
        exit(errno);    
    }
    if(connect(s, ai->ai_addr, ai->ai_addrlen) < 0) {
        printf("Unable to connect to server, err=%d\n",errno);
        exit(errno);
    }   
    if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &size, sizeof(int)) < 0) {
        printf("Error setting options, errno=%d\n",errno);
        exit(errno);
    }
    freeaddrinfo(ai);
    return s;
}

void setsizes(int nbd, uint64_t size64, int blocksize, uint32_t flags)
{
	unsigned long size,xsize;
	int read_only = (flags & NBD_FLAG_READ_ONLY) ? 1 : 0;
	if (ioctl(nbd, NBD_SET_BLKSIZE, 4096UL) < 0) printf("Ioctl/1.1a failed: %d\n",errno);
	size = (unsigned long)(size64>>12);
	if (ioctl(nbd, NBD_SET_SIZE_BLOCKS, size) < 0) {
            syslog(LOG_ERR,"Error setting device size=%d",errno);
        }
	if (ioctl(nbd, NBD_SET_BLKSIZE, (unsigned long)blocksize) < 0) printf("Ioctl/1.1c failed: %d\n",errno);
	ioctl(nbd, NBD_CLEAR_SOCK);
	ioctl(nbd, NBD_SET_FLAGS, (unsigned long) flags);
	if (ioctl(nbd, BLKROSET, (unsigned long) &read_only) < 0) printf("Unable to set read-only attribute for device\n");       
	ioctl(nbd, BLKGETSIZE, &xsize);
}

void negotiate(int sock, uint64_t *rsize64, uint32_t *flags, char* name, uint32_t needed_flags, uint32_t client_flags, uint32_t do_opts)
{
    uint64_t magic, size64;
    uint16_t tmp;
    char buf[256] = "\0\0\0\0\0\0\0\0\0";
    uint32_t opt;
    uint32_t namesize;        

    syslog(LOG_INFO,"Client negotiation");
    if (read(sock, buf, 8) < 0) {
        syslog(LOG_ERR,"Failed to read PASSWD, err=%d",errno);
        return;
    }
    if (strlen(buf)==0) {
        syslog(LOG_ERR,"Zero length PASSWD, err=%d",errno);
        return;
    }
    if (strcmp(buf, INIT_PASSWD)) {
        syslog(LOG_ERR,"Bad PASSWD, err=%d",errno);
        return;
    }
    if (read(sock, &magic, sizeof(magic)) < 0) {
        syslog(LOG_ERR,"Error reading MAGIC, err=%d",errno);
        return;
    }
    magic = ntohll(magic);
    if (magic != OPTS_MAGIC) {
        syslog(LOG_ERR,"Bad MAGIC, err=%d",errno);
        return;
    }
    if(read(sock, &tmp, sizeof(uint16_t)) < 0) {
        syslog(LOG_ERR,"Error reading flags, err=%d",errno);
        return;
    }
    *flags = ((uint32_t)ntohs(tmp));
    client_flags = htonl(client_flags);
    if (write(sock, &client_flags, sizeof(client_flags)) < 0) {
        syslog(LOG_ERR,"Error returning client flags, err=%d",errno);
        return;
    }
    magic = htonll(OPTS_MAGIC);
    if (write(sock, &magic, sizeof(magic)) < 0) {
        syslog(LOG_ERR,"Error writing MAGIC, err=%d",errno);
        return;
    }
    opt = ntohl(NBD_OPT_EXPORT_NAME);
    if (write(sock, &opt, sizeof(opt)) < 0) {
        syslog(LOG_ERR,"Error writing command, err=%d",errno);
        return;        
    }
    namesize = (uint32_t)strlen(name);
    namesize = ntohl(namesize);
    if (write(sock, &namesize, sizeof(namesize)) < 0) {
        syslog(LOG_ERR,"Error writing share name, err=%d",errno);
        return;        
    }
    if (write(sock, name, strlen(name)) < 0) {
        syslog(LOG_ERR,"Error writing share name length, err=%d",errno);
        return;                
    }
    syslog(LOG_ALERT,"SENT NAME=%s",name);
    if (read(sock, &size64, sizeof(size64)) < 0) {
        syslog(LOG_ERR,"Error reading size, err=%d",errno);
        return;                
    }
    size64 = ntohll(size64);
    syslog(LOG_INFO,"Share name is %s, size is %luMB",name,(unsigned long)(size64>>20));
    if(read(sock, &tmp, sizeof(tmp)) < 0) {
        syslog(LOG_ERR,"Error reading flags, err=%d",errno);
        return;                
    }
    *flags |= (uint32_t)ntohs(tmp);
    if (read(sock, &buf, 124) < 0) {
        syslog(LOG_ERR,"Error reading zeros, err=%d",errno);
        return;                
    }
    *rsize64 = size64;
}

int doSetup(char* host,char* name,int nbd,uint64_t *size64)
{
    uint32_t flags;
    uint32_t needed_flags=0;
    uint32_t cflags=0;
    uint32_t opts=0;
    int blocksize=1024;
    int s;
    //
    s = doConnect(host);
    negotiate(s, size64, &flags, name, needed_flags, cflags, opts);
    setsizes(nbd, *size64, blocksize, flags);    
    return s;
}

void bye()
{
    syslog(LOG_INFO,"** Process %d exited\n",getpid());
}

void termination_handler (int signum)
{
    running = False;
}

process * doServe(char* host,char* name)
{
    if(!host) return;
    
    char  nbdpath[64];
    int sock, nbd, f, status;
    struct sigaction new_action;
    
    int dev = getNextFreeDev();
    if(dev == -1) {
	printf("Ran out of NBD devices\n");
	return;
    }
    
    printf("Serving host (%s) device /dev/nbd%d\n",host,dev);

    snprintf(nbdpath,sizeof(nbdpath),"/dev/nbd%d",dev);
    nbd = open(nbdpath, O_RDWR);
    if (nbd < 0) {
        printf("Cannot open NBD\nPlease ensure the 'nbd' module is loaded.\n");
        return;
    }
    ioctl(nbd, NBD_SET_TIMEOUT, (unsigned long)NBD_CLIENT_TIMEOUT);
    //
    f = fork();
    if(f<0) { printf("Fork error [err=%d]\n",errno); exit(1); }
    if(f>0) {
        printf("Forked process %d\n",f);
	procs[pcount].pid = f;
	procs[pcount].nbd = nbd;
	procs[pcount].dev = dev;
	pcount++;
	return (process*)&(procs[pcount-1]);
    }
    //
    setsid();   
    signal( SIGCHLD, SIG_IGN );     
    
    new_action.sa_handler = termination_handler;
    sigemptyset (&new_action.sa_mask);
    new_action.sa_flags = 0;
    sigaction (SIGINT, &new_action, NULL);
    atexit(bye);
    
    uint64_t size;
    char pbuf[128];
    
    do {
        sock = doSetup(host,name,nbd,&size);
        if(sock<0) {
            syslog(LOG_ALERT,"Unable to connect to host [%s] - retry in 10s (%d:%d)",host,sock,errno);
	    sleep(30);
	    continue;
        }
        sprintf(pbuf,"Negotiated size=%lld",(unsigned long long) size);
        doLog(pbuf);
        procs[pcount-1].siz = size;
        if (ioctl(nbd, NBD_SET_SOCK, sock) < 0) {
            syslog(LOG_ALERT,"Unable to connect sockt to [%s]",nbdpath);
	    close(sock);
	    sleep(30);
	    continue;
        }                    
        ioctl(nbd, NBD_DO_IT);
        close(sock);
	if( running ) sleep(5);
        
    } while( running );
    exit(0);
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
	
    char    *name;	
    char    path[256];
    int     status = False;
    int     working = True;
    int64_t size = 0;        

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
		
            doLog("EXPORT NAME");
            doLog(name);

            p1 = doServe(host1,name);
            sleep(2);
            p2 = doServe(host2,name);
            sleep(2);
                
            sprintf(path1,"/dev/nbd%d",p1->dev);
            sprintf(path2,"/dev/nbd%d",p2->dev);    

            fd1 = open(path1,O_RDWR|O_EXCL);
            if(fd1<0) doError("Unable to open RAID10-1");
            else doLog("Opened RAID-1 device");
            fd2 = open(path2,O_RDWR|O_EXCL); 
            if(fd2<0) doError("Unable to open RAID10-2");
            else doLog("Opened RAID-2 device");
                
			working = False;
			status = True;
			free(name);
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
	
    ioctl(fd1, BLKGETSIZE, &size);
    size = htonll(size*512);
    int16_t small = htons(1);
    char zeros[124];
    memset(zeros,0,sizeof(zeros));
    putBytes(sock,&size,sizeof(size));
    putBytes(sock,&small,sizeof(small));
    putBytes(sock,&zeros,sizeof(zeros));
    doLog("Exit NEGOTIATION [Ok]");
    return True;
}

//	doSession - process a single client session

void doSession(int sock)
{
	struct nbd_request request;
	struct nbd_reply reply;
	char buffer[1024*132];
	int readlen,bytes;
	int running = True;

	doLog("Enter SESSION");
	doConnectionMade(sock);
        
        int h1,h2,h3;
        
	if(doNegotiate(sock)) {
	
		doLog("Processing DATA requests ...");
                h1 = fd1;
                h2 = fd2;
        		        
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
					while( len > 0 ) {
                                            if(lseek(h1,off,SEEK_SET)==-1) running = doError("SEEK");                                           
					    readlen = len > sizeof(buffer)?sizeof(buffer):len;
					    bytes = read(h1,&buffer,readlen);
                                            if( bytes != readlen ) running = doError("READ");
					    else putBytes(sock,&buffer,readlen);
                                            len -= readlen;
                                            off += readlen;
                                            h3 = h1;
                                            h1 = h2;
                                            h2 = h3;
					}
				break;
                
			case NBD_WRITE:
				while( len > 0 ) {
					readlen = len > sizeof(buffer)?sizeof(buffer):len;
				        getBytes(sock,&buffer,readlen);
                                        
                                        if(lseek(h1,off,SEEK_SET)==-1) running = doError("SEEK");
                                        if(lseek(h2,off,SEEK_SET)==-1) running = doError("SEEK");
                                        
					bytes = write(h1,&buffer,readlen);
                                        bytes = write(h2,&buffer,readlen);
                                        
					if( bytes != readlen ) running = doError("WRITE");
					len -= readlen;
				}
				putBytes(sock,&reply,sizeof(reply));
				break;
            
			case NBD_CLOSE:
				//putBytes(&reply,sizeof(reply));
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
	int net,f,status,sock;
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
				if(!debug) {
					f = fork();
					if(f<0) { printf("Fork error [err=%d]\n",errno); exit(1); }
					if(f==0) {
						syslog(LOG_INFO,"Forkd new process with id = %d",getpid());
						setsid();
						for (f=getdtablesize();f>=0;--f) if(f!=sock) close(f);
						f=open("/dev/null",O_RDWR); status=dup(f); status=dup(f); umask(027); status=chdir("/tmp/");
						doSession(sock);
						close(sock);
						syslog(LOG_INFO,"Process with pid %d terminated",getpid());
						exit(0);
					}
				} else {
					doSession(sock);
				}
				close(sock);
			}
		}	
	}
	doLog("Exit ACCEPT");
}

//	main - just what it says on the can ...
  
void main(int argc,char **argv)
{
    int listener,c,f,status;
    struct sigaction new_action;
 	
    while ((c = getopt (argc, argv, "da:b:n:")) != -1)
    {
        switch(c)
    	{
	    case 'd':
                debug++;
                break;
            case 'a':
                host1 = optarg;
                break;
            case 'b':
                host2 = optarg;
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
	f=open("/dev/null",O_RDWR); status=dup(f); status=dup(f); umask(027); status=chdir("/tmp/");
	signal( SIGCHLD, SIG_IGN ); 
    }
    openlog ("nbd-client", LOG_CONS|LOG_PID|LOG_NDELAY , LOG_USER);
    doLog("NBD client v0.1 started");   
                
    //new_action.sa_handler = termination_handler;
    //sigemptyset (&new_action.sa_mask);
    //new_action.sa_flags = 0;
    //sigaction (SIGINT, &new_action, NULL);   
    //atexit(doKill);
    
    if((listener=getSocket())>0) {
	doAccept(listener);
	close(listener);
    }
    doLog("NBD server stopped");
    closelog ();
    exit(0); 
}
