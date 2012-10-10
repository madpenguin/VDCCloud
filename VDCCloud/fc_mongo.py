#!/usr/bin/python
#
#	fc_mongo - library for accessing mongoDB
#
###############################################################################
#
import os
import sys
import pymongo
from datetime import datetime
#
HOSTNAME 	= os.uname()[1].split(".")[0]
MAX_BLOCK	= 64				# max NBD devices we're going to allow
#
class Database:
	""" Class to manage the MongoDB Connection """
	def __init__(self):
		#
		#	TODO :: Add Error trapping / retries!
		#
		mongo = pymongo.ReplicaSetConnection(
			"data2:27017,data1:27017",
			replicaSet="linux",slaveOk=True,
			safe=True)

		self.db = mongo.fc_gluster
		
	def addRaid(self,name,port,cache,cache_size,vol,vol_size,hosts):
		""" add a new instance """
		now = datetime.utcnow()
		record = {
			"name"		: name,
			"port"		: port,
			"node"		: HOSTNAME,
			"cache"		: cache,
			"cache_size"	: cache_size,			
			"disabled"	: False,
			"created"	: now,
			"modified"	: now,
			"vol"		: vol,
			"vol_size"	: vol_size,
			"type"		: 'RAID',
			"hosts"		: hosts
		}
		self.db.instances.save(record)

	def getDevice(self,host,name):
		""" retrieve the device name for an instance on a host """
		record = {
			"node"	: host,
			"name"	: name
		}
		record = self.db.devices.find_one(record)
		if record and record.has_key('device'): return record['device']
		return None

	def getDevices(self,host,name):
		""" retrieve the device name for an instance on a host """
		record = {
			"node"	: host,
			"name"	: name
		}
		record = self.db.devices.find_one(record)
		if record: return record['devices']
		return None

	def delDevice(self,host,name):
		""" retrieve the device name for an instance on a host """
		record = {
			"node"	: host,
			"name"	: name
		}
		return self.db.devices.remove(record)
	
	def setDevice(self,host,name,dev):
		""" update a device entry """
		search = {
			"node"	: host,
			"name"	: name,
		}
		update = {
			"node"	: host,
			"name"	: name,
			"device": dev	
		}
		return self.db.devices.find_and_modify(search, update , new=True, upsert=True);		
		
	def setType(self,name,typ):
		""" change the type of an instance """
		search = {
			"name"	: name,
		}
		update = {
			"$set":{ "type": typ } 
		}
		return self.db.instances.find_and_modify(search, update , new=False, upsert=False);

	def setHosts(self,name,hosts):
		""" change serving hosts for instance """
		search = {
			"name"	: name,
			"node"  : HOSTNAME,
		}
		nodes = {}
		for host in hosts.keys():
			nodes['hosts.'+host] = True
		update = { "$set": nodes }
		return self.db.instances.find_and_modify(search, update , new=False, upsert=False);
		
	def setDevices(self,inst):
		
		search = {
			"node"	: HOSTNAME,
			"name"	: inst['name'],
		}
		devices = []		
		for host in inst['hosts']:
			devices.append(self.getNextDevice(devices))
		
		update = { "$set": { "devices" : devices } }
		if not self.db.devices.find_and_modify(search, update , new=True, upsert=True):
			print "Failed to update devices"
			return False
		return True

	def getNextDevice(self,existing=[]):
		""" find the next free NBD device """
		files = []		
		for device in existing:
			path = "/dev/%s" % device
			files.append(path)
			

		record = {
			"node"	: HOSTNAME
		}
		results = self.db.devices.find(record)
		for result in results:
			if result.has_key('devices'):
				for dev in result['devices']:
					path = "/dev/%s" % dev
					files.append(path)
		
		pids=os.listdir('/proc') 
		for pid in sorted(pids): 
			try: 
				int(pid) 
			except ValueError: 
				continue 
			fd_dir=os.path.join('/proc', pid, 'fd') 
			for file in os.listdir(fd_dir): 
				try: 
					link=os.readlink(os.path.join(fd_dir, file)) 
				except OSError: 
					continue 
				files.append(link)
		
		index = 0
		while index < MAX_BLOCK:
			path = "/dev/nbd%d" % index
			if not path in files: return "nbd%d" % index
			index += 1
			
		print "No BLOCK devices left"
		exit()

	def getInstance(self,name):
		""" retrieve the device name for an instance on a host """
		return self.db.instances.find_one({ "name" : name })
		
	def getInstances(self,host):
		""" retrieve list of instances for given node """
		if not host:
			return self.db.instances.find()
		else:	return self.db.instances.find({"node":host})

	def getHost(self,host):
		return self.db.hosts.find_one({'host':host})

	def procRegister(self,host,name,pid):
		""" track a running process """
		now = datetime.utcnow()
		search = {
			"name"		: name,
			"host"		: host,
		}		
		update = {
			"name"		: name,
			"host"		: host,
			"pid"		: pid,
			"start"		: now
		}
		self.db.processes.find_and_modify(search, update , new=True, upsert=True)

	def procQuery(self,host,name):
		record = {
			"name"		: name,
			"host"		: host,			
		}
		return self.db.processes.find_one(record)

	def procKill(self,host,name):
		record = {
			"name"		: name,
			"host"		: host,			
		}
		return self.db.processes.remove(record)
	
	def procRunning(self,host,name):
		search = {
			"name"		: name,
			"host"		: host,
		}		
		result = self.db.processes.find_one(search)
		if not result: return False
		path = "/proc/%d" % result['pid']
		if not os.path.exists(path): return False
		return True
	
	def regClient(self,host,name,device,server):
		now = datetime.utcnow()
		search = {
			"name"		: name,
			"host"		: host,
			"device"	: device,
		}
		update = {
			"name"		: name,
			"host"		: host,
			"device"	: device,
			"server"	: server,
			"start"		: now
		}
		self.db.processes.find_and_modify(search, update , new=True, upsert=True)

	def unregClient(self,host,name,device):
		now = datetime.utcnow()
		search = {
			"name"		: name,
			"host"		: host,
			"device"	: device,
		}
		self.db.processes.remove(search)
		
	def getClient(self,host,name,device):
		search = {
			"name"		: name,
			"host"		: host,
			"device"	: device,
		}
		return self.db.processes.find_one(search)
	
	def getNBDs(self,host,name):
		search = {
			"name"		: name,
			"host"		: host,
		}
		results = self.db.processes.find(search)
		devices = []
		for result in results:
			devices.append(result['device'])

		return devices
	
	def setLocal(self,name):
		search = { "name" : name }
		update = { "$set" : { "host" : HOSTNAME }}
		self.db.instances.find_and_modify(search, update , new=False, upsert=False)