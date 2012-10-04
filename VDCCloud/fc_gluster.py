#!/usr/bin/python
#
#	TODO:: convert /etc/fc_gluster into a persistent mapping
#	TODO:: modprobe for flashcache and nbd @ start
#	TODO:: fix auto-boot on node1 and node2 re; mount gluster
# 	TODO:: submit patches for FC re; more stats info
#
from sys import argv,exit
#######
import sys
import socket
import subprocess as sub
import os
import stat
import time
import shelve
import libvirt
from libvirt import VIR_MIGRATE_LIVE,VIR_MIGRATE_PEER2PEER,VIR_MIGRATE_PERSIST_DEST,VIR_MIGRATE_UNDEFINE_SOURCE
###############################################################################
#
#	INITIALISATION
#
###############################################################################
CONFIG_DIR = '/etc/fc_gluster/'
HOSTNAME = os.uname()[1]
DATABASE = '/etc/flashcache.dbm'
FALLOW_DELAY = 300
#
#	Routine to obtain colour codes ...
#
def getColor(number):
	""" there must be a better way to do this!! """
	pipe = sub.Popen("tput setaf "+str(number),bufsize=8192,stdout=sub.PIPE,stderr=sub.PIPE,shell=True)
	return pipe.communicate()[0]
#
#
#
sys.path.append(CONFIG_DIR)
red 	= getColor(1)
blue 	= getColor(4)
yellow 	= getColor(3)
green 	= getColor(2)
x 		= getColor(7)
#
#	Open Persistent Database
#
db = shelve.open(DATABASE,flag='c',protocol=None,writeback=True)
#
###############################################################################
#
def isKVMRunning(io,inst):

	active = []
	for id in io.listDomainsID():
		domain = io.lookupByID(id)
		info = domain.info()
		if info[0] == libvirt.VIR_DOMAIN_RUNNING: active.append(domain.name())
	return inst in active
#
###############################################################################
#
def getDomain(io,inst):
	active = []
	for id in io.listDomainsID():
		domain = io.lookupByID(id)
		if domain.name() == inst: return domain
	return None
#
###############################################################################
#
def delay(secs):
	while secs > 0:
		logMiddle(secs,blue)
		time.sleep(10)
		secs -= 10
#
###############################################################################
#
def getInstances():
	""" return the names of the instances to process """

	if len(argv)>2:	return argv[2:]
	else:
		instances = []
		for path in [CONFIG_DIR]:
			files = os.listdir(path)
			for inst in files:
				[inst,ext]= inst.split(".")
				if ext=="py": instances.append(inst)

		return instances
#
###############################################################################
#
def doLoad(inst):
	""" load the dynamic configuration for a given instance """
	try:
		config = __import__(inst)
	except ImportError:
		print "No configuration for ",inst
		exit(1)
	return config
#
###############################################################################
#
def logBegin(text):
	""" start logging a line to stdout """
	print "%s * %s%s%s%s" % (blue,x,green,text,x),
	sys.stdout.flush()
#
###############################################################################
#
def logEnd(text,ok):
	""" stop logging and print a status message """
	templ = "[%s%s%s]"
	if ok:
			print templ % (green,text,x)
	else:	print templ % (red,text,x)
#
###############################################################################
#
def logMiddle(text,col):
	print "[%s%s%s] " % (col,text,x),
	sys.stdout.flush()
#
###############################################################################
#
def logYN(text):
	while True:
		logBegin(text+" %s(y/N)%s " % (yellow,x))
		choice = raw_input().lower()
		if not choice in ['y','N','']: continue
		if choice == 'y': return True
		return False
#
###############################################################################
#
def isRunning(inst):
	return os.path.exists("/dev/mapper/"+inst)
#
###############################################################################
#	
def setParam(inst,config,param,v):
	""" set a flashcache parameter """
	path = "/proc/sys/dev/flashcache/"+inst+"+"+config.dev+"/"+param
	if os.path.exists(path):
		print "[%s%s%s=%s%s] " % (yellow,param,green,v,x),
		io = open(path,"w")
		io.write(str(v)+"\n")
		io.close()
#
###############################################################################
#	
def run(cmd):
	""" run a shell command in a sub-shell """
	pipe = sub.Popen(cmd,bufsize=8192,stdout=sub.PIPE,stderr=sub.PIPE,shell=True)
	result = pipe.communicate()
	return pipe.returncode
#
def remote(cmd):
	""" run a shell command in a sub-shell """
	pipe = sub.Popen(cmd,bufsize=8192,stdout=sub.PIPE,stderr=sub.PIPE,shell=True)
	result = pipe.communicate()
	return [pipe.returncode,result]
#
###############################################################################
#	
class Settings():
	""" dummy class for stats - TODO:: get rid of this! """
	def __init__(self):
		pass
#
###############################################################################
#
def do_stats():
	""" print out our status in tabular format """
	print "+"+25*"-"+"+"+7*"-"+"+"+17*"-"+"+"+17*"-"+"+"+17*"-"+"+"+17*'-'+'+'
	print "| %s%-14s%s %s%6s%s   | %s%-5s%s | %s%15s%s | %s%15s%s | %s%15s%s | %s%15s%s |" % (blue,"Instance",x,green,"Size",x,blue,"Raw",x,blue,"Cached",x,blue,"Dirty",x,blue,"Read",x,blue,'Write',x)
	print "+"+25*"-"+"+"+7*"-"+"+"+17*"-"+"+"+17*"-"+"+"+17*"-"+"+"+17*'-'+'+'
	for inst in getInstances(): 
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		if db.has_key(inst):
			config.ssd,config.dev = db[inst]
		else:	config.ssd,config.dev = ['None','None']

		if not isRunning(inst):
			print "| %s%-23s%s | %s%-5s%s |    %s***%s down %s***%s | %51s |" % (red,inst,x,yellow,config.dev,x,red,blue,red,x,' ')
			continue

		stats = ""
		if config.dev:
			path = "/proc/flashcache/"+inst+"+"+config.dev+"/flashcache_stats"
			if os.path.exists(path): 
				io = open(path,"r")
				stats = io.read()
				io.close()

		cmd = "dmsetup table %s" % inst
		pipe = sub.Popen(cmd,bufsize=8192,stdout=sub.PIPE,stderr=sub.PIPE,shell=True)
		result = pipe.communicate()
		if pipe.returncode:
			print " %s*%s - FAILURE RUNNING DMSETUP for %s%s%s" % (red,x,yellow,inst,x)
			sys.exit(0)

		result = result[0]
		result = result.expandtabs(0).split("\n")
		record = result[1].split(",")
		ssd = record[0].split("(")[1].replace(")","")
		aoe = record[1].split("(")[1].replace(")","").split(" ")[0]
		record = result[2].split(",")
		capacity = record[0].split("(")[1].replace(")","")
		record  = result[3].split(",")
		try:
			cached  = int(record[1].split("(")[1].replace(")",""))
			percent = int(record[2].split("(")[1].replace(")",""))
			record  = result[4].split(",")
			dirtyb = int(record[0].split("(")[1].replace(")",""))
			dirtyp = int(record[1].split("(")[1].replace(")",""))
		except:
			record  = result[4].split(",")
			cached  = int(record[1].split("(")[1].replace(")",""))
			percent = int(record[2].split("(")[1].replace(")",""))
			record  = result[5].split(",")
			dirtyb = int(record[0].split("(")[1].replace(")",""))
			dirtyp = int(record[1].split("(")[1].replace(")",""))

		z = Settings()
		stats = stats.replace("\n"," ")
		stats = stats.split(" ")
		for stat in stats:
			if len(stat)<2: continue
			key,val = stat.split("=")
			setattr(z,key,int(val))

		total_reads = z.ssd_reads + z.disk_reads + 1
		rp = 100 * float(z.ssd_reads) / float(total_reads)
		total_writes = z.ssd_writes + z.disk_writes + 1
		wp = 100 * float(z.ssd_writes) / float(total_writes)

		cach = "%s%8d%s (%s%3d%%%s) | " % (green,cached,x,yellow,percent,x)
		dirt = "%s%8d%s (%s%3d%%%s) | " % (green,dirtyb,x,yellow,dirtyp,x)
		read = "%s%8d%s (%s%3d%%%s) | " % (green,total_reads,x,yellow,rp,x)
		writ = "%s%8d%s (%s%3d%%%s) | " % (green,total_writes,x,yellow,wp,x)

		print "| %s%-14s%s (%s%6s%s) | %s%-5s%s | " % (green,inst,x,yellow,capacity,x,green,config.dev,x)+cach+dirt+read+writ

	print "+"+25*"-"+"+"+7*"-"+"+"+17*"-"+"+"+17*"-"+"+"+17*"-"+"+"+17*'-'+'+'
#
###############################################################################
#
def do_stop():
	""" stop one or more instances """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Stopping %s%-12s%s :: ' % (yellow,inst,x))

		if not isRunning(inst):
			logEnd('already stopped',False)
			continue

		if not db.has_key(inst):
			logEnd('unknown instance',False)
			continue
	
		config.ssd,config.dev = db[inst]

		if run("/sbin/dmsetup remove %s" % inst):
			logEnd('dmsetup Failed',False)
		else: 	
			logMiddle('dm stopped',green)
			if run("qemu-nbd -d /dev/"+config.dev):
				logMiddle(config.dev,red)
			else:	
				logMiddle(config.dev,blue)

		logEnd('Ok',True)

###############################################################################
#
def do_remove():
	""" stop one or more instances """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Removing %s%-12s%s :: ' % (yellow,inst,x))

		if not isRunning(inst):
			logEnd('already stopped',False)
			continue

		if not db.has_key(inst):
			logEnd('unknown instance',False)
			continue
	
		config.ssd,config.dev = db[inst]

		if run("/sbin/dmsetup remove %s" % inst):
			logEnd('dmsetup Failed',False)
			continue
		else: 	
			logMiddle('dm stopped',green)
			if run("qemu-nbd -d /dev/"+config.dev):
				logMiddle(config.dev,red)
			else:	
				logMiddle(config.dev,blue)

		setParam(inst,config,"do_sync",1)

		if run("lvremove -f "+config.ssd):
				logEnd('Fail',False)
		else:	logEnd('Ok',True)

###############################################################################
#
def do_start():
	""" start one or more instances """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Starting %s%-12s%s :: ' % (yellow,inst,x))

		blockdev = "/dev/mapper/"+inst
		if os.path.exists(blockdev):
			logEnd('already started',False)
			continue

		img = "/"+config.FS+"/"+inst+".img"
		if not os.path.exists(img):
			logEnd('missing: '+img,False)
			continue

		if db.has_key(inst):
			config.ssd,config.dev = db[inst]
			logMiddle('old cache',blue)
		else:
			logMiddle('new cache',green)
			used = []
			config.ssd = "/dev/"+config.VG+"/"+inst
			for entry in db:
				ssd,dev = db[entry]
				index = int(dev[3:])
				used.append(index)

			index = 0
			while index in used: index += 1
			config.dev = 'nbd'+str(index)
			db[inst] = [config.ssd,config.dev]

		if run("qemu-nbd -c /dev/"+config.dev+" "+img):	
			logEnd(config.dev,Fail)
			continue

		logMiddle(config.dev,blue)

		if os.path.exists(config.ssd):
			logMiddle('load',blue)
			if run("/sbin/flashcache_load "+config.ssd+" "+inst):
				logEnd('fail',False)
				continue
		else:
			logMiddle('lvm',blue)
			if run("lvcreate -L%dG -n%s %s" % (config.SIZE,inst,config.VG)):
				logEnd('fail',False)
				continue
			
			logMiddle('ndb',blue)
			if run("/sbin/flashcache_create -p back -s%dg %s %s /dev/%s" % (config.SIZE,inst,config.ssd,config.dev)):
				logEnd('fail',False)
				continue

		logMiddle('ReadAhead::8192',blue)
		if run("blockdev --setra 8192 /dev/mapper/"+inst): logMiddle('fail',red)
		setParam(inst,config,"reclaim_policy",1)
		setParam(inst,config,"fallow_delay",FALLOW_DELAY)
		logEnd('Ok',green)
#
###############################################################################
#
def do_sync():
	""" synchronise all instances with their back-ends """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Synchronizing %s%-12s%s' % (yellow,inst,x))
		config.ssd,config.dev = db[inst]
		if isRunning(inst):
			setParam(inst,config,"fallow_delay",FALLOW_DELAY)
			setParam(inst,config,"do_sync",1)
			logEnd('Ok',True)
		else:
			logEnd('Fail',False)
#
###############################################################################
#
def do_nosync():
	""" synchronise all instances with their back-ends """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Setting local cache mode on %s%-12s%s' % (yellow,inst,x))
		config.ssd,config.dev = db[inst]
		if isRunning(inst):
			setParam(inst,config,"do_sync",1)
			setParam(inst,config,"fallow_delay",0)
			logEnd('Ok',True)
		else:
			logEnd('Fail',False)
#
###############################################################################
#
def do_cache(v):
	""" turn caching on or for for one or more back-ends """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Change caching %s%-12s%s' % (yellow,inst,x))
		config.ssd,config.dev = db[inst]
		if isRunning(inst):
			setParam(inst,config,'cache_all',v)
			setParam(inst,config,"do_sync",1)
			setParam(inst,config,"reclaim_policy",1)
			logEnd('Ok',True)
		else:
			logEnd('Fail',False)
#
###############################################################################
#
def do_cacheoff(): 
	""" turn caching off """
	do_cache(0)
#
###############################################################################
#
def do_cacheon():  
	""" turn caching on """
	do_cache(1)
#
###############################################################################
#
def do_clearstats():
	""" clear statistics """
	for inst in getInstances():
		config = doLoad(inst)
		if config.NODE <> HOSTNAME: continue
		logBegin('Clearing stats %s%-12s%s' % (yellow,inst,x))
		config.ssd,config.dev = db[inst]
		if isRunning(inst):
			setParam(inst,config,'zero_stats',1)
			logEnd('Ok',True)
		else:
			logEnd('Fail',False)
#
###############################################################################
#
def do_migrate(pingpong=False):
	""" migrate a running instance from one machine to another """
	#
	#	Params: instance dst
	#
	if len(argv)<4:
		logBegin('Migration needs <instance> and <destination>')
		logEnd('Fail',False)
		return

	inst = sys.argv[2]
	dest = sys.argv[3]

	logBegin('Validating source')
	src_conn = libvirt.open("qemu:///system")

	if not isKVMRunning(src_conn,inst):
		if not pingpong:
			logMiddle('Instance is not running',blue)
			logEnd('Fail',False)
			return

		logEnd('Ok',True)

		while True:
			logBegin('Waiting to see instance')
			if isKVMRunning(src_conn,inst): 
				delay(10)
				break
			delay(40)
			logEnd('Not found',True)

	logEnd('Ok',True)

	logBegin('Validating destination')
	dst_conn = libvirt.open("qemu+ssh://"+dest+"/system")
	if isKVMRunning(dst_conn,inst):
		logMiddle('Instance is already running',blue)
		logEnd('Fail',False)
		return

	logEnd('Ok',True)

	logBegin('Turning caching off on source')
	config = doLoad(inst)
	config.ssd,config.dev = db[inst]
	setParam(inst,config,'cache_all',0)
	setParam(inst,config,"do_sync",1)
	logEnd('Ok',True)

	logBegin('Checking mapper on %s%s%s' % (yellow,dest,x))
	blockdev = "/dev/mapper/"+inst

	status,result = remote('ssh root@'+dest+' "if [ -e '+blockdev+' ]; then echo -n 1; else echo -n 0; fi"')
	if status:
		logEnd(result,False)
		return

	logEnd('Ok',True)

	if result[0]=='1':
		yn = logYN('Mapper exists on target, remove?')
		if not yn:
			logBegin('Aborting procedure')
			logEnd('Fail',False);
			return

		logBegin('Shutting down Mapper on %s%s%s' % (yellow,dest,x))
		status,result = remote('ssh root@'+dest+' "service fc_gluster remove '+inst+'"')
		if status:
			logEnd('Failed',False)
			print result
			return
	
		logEnd('Ok',True)

	logBegin('Creating Mapper on %s%s%s' % (yellow,dest,x))
	status,result = remote('ssh root@'+dest+' "service fc_gluster start '+inst+'"')
	if status:
		logEnd('Failed',False)
		print result
		return
	
	logEnd('Ok',True)

	logBegin('Waiting for Dirty Blocks => 0')
	while True:
		cmd = "dmsetup table %s" % inst
		pipe = sub.Popen(cmd,bufsize=8192,stdout=sub.PIPE,stderr=sub.PIPE,shell=True)
		result = pipe.communicate()
		if pipe.returncode:
			print " %s*%s - FAILURE RUNNING DMSETUP for %s%s%s" % (red,x,yellow,inst,x)
			sys.exit(0)
	
		result = result[0]
		result = result.expandtabs(0).split("\n")
		record = result[1].split(",")
		ssd = record[0].split("(")[1].replace(")","")
		aoe = record[1].split("(")[1].replace(")","").split(" ")[0]
		record = result[2].split(",")
		capacity = record[0].split("(")[1].replace(")","")
		record  = result[3].split(",")
		try:
			cached  = int(record[1].split("(")[1].replace(")",""))
			percent = int(record[2].split("(")[1].replace(")",""))
			record  = result[4].split(",")
			dirtyb = int(record[0].split("(")[1].replace(")",""))
			dirtyp = int(record[1].split("(")[1].replace(")",""))
		except:
			record  = result[4].split(",")
			cached  = int(record[1].split("(")[1].replace(")",""))
			percent = int(record[2].split("(")[1].replace(")",""))
			record  = result[5].split(",")
			dirtyb = int(record[0].split("(")[1].replace(")",""))
			dirtyp = int(record[1].split("(")[1].replace(")",""))

		if dirtyb == 0: 
			logEnd('Ok',True)
			break

		logMiddle('wait',blue)
		setParam(inst,config,"do_sync",1)
		time.sleep(1)

	logBegin('Waiting')
	delay(10)
	logEnd('Ok',True)

	logBegin('Migrating %s%s%s to %s%s%s' % (yellow,inst,x, yellow,dest,x))

	flags = VIR_MIGRATE_LIVE|VIR_MIGRATE_PEER2PEER|VIR_MIGRATE_PERSIST_DEST|VIR_MIGRATE_UNDEFINE_SOURCE
	uri = "qemu+ssh://%s/system" % (dest)
	src = getDomain(src_conn,inst)

	dst = src.migrate(dst_conn,flags,inst,uri,0)

	if not dst:
		logEnd('Failed',False)
		print result
		return
	
	logEnd('Ok',True)

	if not pingpong:
		yn = logYN('Are we Ok to remove the local Mapper?')
		if not yn:
			logBegin('Aborting procedure')
			logEnd('Fail',False);
			return

	logBegin('Shutting down obsolete Mapper')
	if run('service fc_gluster remove '+inst):
		logEnd('Failed',False)
		print result
		return
	
	logEnd('Ok',True)

	logBegin('Turning cache back on')
	status,result = remote('ssh root@'+dest+' "service fc_gluster cacheon '+inst+'"')
	if status:
		logEnd('Failed',False)
		print result
		return
	
	logEnd('Ok',True)
#
###############################################################################
#
def do_pingpong():
	while True:
		logBegin("Sleeping")
		delay(20)
		logEnd("Ok",True)
		print
		do_migrate(pingpong=True)
#
###############################################################################
#
def do_usage():
	print "Usage:"
	print "%s {start|stop|stats|clearstats|remove|sync|cacheon|cacheoff|migrate|pingpong}" % (argv[0])
	exit(1)
#
###############################################################################
#
routines = {
	'stats'			:do_stats,
	'clearstats'	:do_clearstats,
	'stop' 			:do_stop,
	'start'			:do_start,
	'remove'		:do_remove,
	'sync'			:do_sync,
	'nosync'		:do_nosync,
	'cacheoff'		:do_cacheoff,
	'cacheon'		:do_cacheon,
	'migrate'		:do_migrate,
	'pingpong'		:do_pingpong,
}

if len(argv)<2:	
		cmd = ""
else:	cmd = argv[1]

if cmd in routines:	
		routines[cmd]()
else:	do_usage()

