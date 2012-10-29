/*
 *      nbd-client.c
 *      (c) Gareth Bult 2012
 *
 *	Specifically this client supports two connections at the same time with
 *	a view to providing links for network RAID10. It also provides scope for
 *	more than two links, and local caching / RAID rebuilds etc ...
 *
 *	TODO :: dynamically allocate NBD devices (on 'next free' basis)
 *	TODO :: open both devices and sit in a r/w loop (RAID10)
 *	TODO :: incorporate server to re-serve information
 *	TODO :: incorporate caching mechanism
 *	TODO :: incorporate RAID / recovory options
 *	TODO :: insert driver code / merge with server
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
#include <unistd.h>
#include <time.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <signal.h>
#include "nbd.h"

int debug;
extern char *optarg;
int running = True;

//typedef struct process {
//    int nbd;
//    int pid;
//    int dev;
//} process;

process procs[8];
int pcount=0;

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
    
    if( getaddrinfo(host, NBD_PORT, &hints, &ai) < 0) {
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

void setsizes(int nbd, uint64_t size64, int blocksize, uint32_t flags) {
	unsigned long size;
	int read_only = (flags & NBD_FLAG_READ_ONLY) ? 1 : 0;
	if (size64>>12 > (uint64_t)~0UL) printf("Device too large.\n");
	else {
		if (ioctl(nbd, NBD_SET_BLKSIZE, 4096UL) < 0) printf("Ioctl/1.1a failed: %d\n",errno);
		size = (unsigned long)(size64>>12);
		if (ioctl(nbd, NBD_SET_SIZE_BLOCKS, size) < 0) printf("Ioctl/1.1b failed: %d\n",errno);
		if (ioctl(nbd, NBD_SET_BLKSIZE, (unsigned long)blocksize) < 0) printf("Ioctl/1.1c failed: %d\n",errno);
		//fprintf(stderr, "bs=%d, sz=%llu bytes\n", blocksize, 4096ULL*size);
	}
	ioctl(nbd, NBD_CLEAR_SOCK);
	/* ignore error as kernel may not support */
	ioctl(nbd, NBD_SET_FLAGS, (unsigned long) flags);
	if (ioctl(nbd, BLKROSET, (unsigned long) &read_only) < 0) printf("Unable to set read-only attribute for device\n");
}

void negotiate(int sock, uint64_t *rsize64, uint32_t *flags, char* name, uint32_t needed_flags, uint32_t client_flags, uint32_t do_opts)
{
	uint64_t magic, size64;
	uint16_t tmp;
	char buf[256] = "\0\0\0\0\0\0\0\0\0";

	//printf("Negotiation: ");
	if (read(sock, buf, 8) < 0)
		err("Failed/1: %m");
	if (strlen(buf)==0)
		err("Server closed connection");
	if (strcmp(buf, INIT_PASSWD))
		err("INIT_PASSWD bad");
	//printf(".");
	if (read(sock, &magic, sizeof(magic)) < 0)
		err("Failed/2: %m");
	magic = ntohll(magic);
	if(name) {
		uint32_t opt;
		uint32_t namesize;

		if (magic != OPTS_MAGIC)
			err("Not enough opts_magic");
		//printf(".");
		if(read(sock, &tmp, sizeof(uint16_t)) < 0) {
			err("Failed reading flags: %m");
		}
		*flags = ((uint32_t)ntohs(tmp));
		if((needed_flags & *flags) != needed_flags) {
			/* There's currently really only one reason why this
			 * check could possibly fail, but we may need to change
			 * this error message in the future... */
			fprintf(stderr, "\nE: Server does not support listing exports\n");
			exit(EXIT_FAILURE);
		}

		client_flags = htonl(client_flags);
		if (write(sock, &client_flags, sizeof(client_flags)) < 0)
			err("Failed/2.1: %m");

		/* Write the export name that we're after */
		magic = htonll(OPTS_MAGIC);
		if (write(sock, &magic, sizeof(magic)) < 0)
			err("Failed/2.2: %m");

		opt = ntohl(NBD_OPT_EXPORT_NAME);
		if (write(sock, &opt, sizeof(opt)) < 0)
			err("Failed/2.3: %m");
		namesize = (uint32_t)strlen(name);
		namesize = ntohl(namesize);
		if (write(sock, &namesize, sizeof(namesize)) < 0)
			err("Failed/2.4: %m");
		if (write(sock, name, strlen(name)) < 0)
			err("Failed/2.4: %m");
	} else {
		if (magic != CLISERV_MAGIC)
			err("Not enough cliserv_magic");
		//printf(".");
	}

	if (read(sock, &size64, sizeof(size64)) < 0)
		err("Failed/3: %m\n");
	size64 = ntohll(size64);

	if ((size64>>12) > (uint64_t)~0UL) {
		printf("size = %luMB", (unsigned long)(size64>>20));
		err("Exported device is too big for me. Get 64-bit machine :-(\n");
	} //else
		//printf("size = %luMB", (unsigned long)(size64>>20));

	if(!name) {
		if (read(sock, flags, sizeof(*flags)) < 0)
			err("Failed/4: %m\n");
		*flags = ntohl(*flags);
	} else {
		if(read(sock, &tmp, sizeof(tmp)) < 0)
			err("Failed/4: %m\n");
		*flags |= (uint32_t)ntohs(tmp);
	}

	if (read(sock, &buf, 124) < 0)
		err("Failed/5: %m\n");
	//printf("\n");

	*rsize64 = size64;
}

int doSetup(char* host,char* name,int nbd)
{
    uint64_t size64;
    uint32_t flags;
    uint32_t needed_flags=0;
    uint32_t cflags=0;
    uint32_t opts=0;
    int blocksize=1024;
    int s;
    //
    s = doConnect(host);
    negotiate(s, &size64, &flags, name, needed_flags, cflags, opts);
    setsizes(nbd, size64, blocksize, flags);    
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

process * doServe(char* spec,char* name)
{
    if(!spec) return;
    
    char* host = strtok(spec,":");
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
    
    do {
        sock = doSetup(host,name,nbd);
        if(sock<0) {
            syslog(LOG_ALERT,"Unable to connect to host [%s] - retry in 10s (%d:%d)",host,sock,errno);
	    sleep(30);
	    continue;
        }
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

//  main - just what it says on the tin!

void main(int argc,char** argv)
{
    int c;
    char* host1=NULL;
    char* host2=NULL;
    char* name =NULL;
    int status;
    struct sigaction new_action;
    process *p1;
    process *p2;
	
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
            case 'n':
                name  = optarg;
                break;
            default:
                exit(1);
	    
	}
    }
    openlog ("nbd-client", LOG_CONS|LOG_PID|LOG_NDELAY , LOG_USER);
    p1 = doServe(host1,name); sleep(1);
    p2 = doServe(host2,name);
        
    new_action.sa_handler = termination_handler;
    sigemptyset (&new_action.sa_mask);
    new_action.sa_flags = 0;
    sigaction (SIGINT, &new_action, NULL);   
    atexit(doKill);
    
    char path1[64];
    char path2[64];
    
    sprintf(path1,"/dev/nbd%d",p1->dev);
    sprintf(path2,"/dev/nbd%d",p2->dev);
    
    int fd1 = open(path1,O_RDWR,O_DIRECT);
    int fd2 = open(path2,O_RDWR,O_DIRECT);

    printf("Main Loop [%s] [%s]\n",path1,path2);
    do
    {
	sleep(1);	
    } while (True);
}