class Database:
	""" Class to manage the MongoDB Connection """
	def __init__(self):
		#
		#	TODO :: Add Error trapping / retries!
		#
		mongo = pymongo.ReplicaSetConnection(
			"data1:27017,data2:27017",
			replicaSet="linux",
			safe=True)

		self.db = mongo.fc_gluster
		#now = datetime.utcnow()
		#record = {
		#	"name"		: 'test1', 
		#	"node"		: 'node1',
		#	"vol_group"	: 'cache',
		#	"path"		: 'encryptec',
		#	"disabled"	: False,
		#	"created"	: now,
		#	"modified"	: now,
		#	"cache_size"	: 1,
		#	"vol_size"	: 10,
		#}
		#self.db.instances.save(record)
		
		#record = {
		#	"node"		: "node1",
		#	"name"		: "varnish1",
		#	"device"	: "nbd4"
		#}
		#self.db.devices.save(record)

	def getDevice(self,host,name):
		""" retrieve the device name for an instance on a host """
		record = {
			"node"	: host,
			"name"	: name
		}
		record = self.db.devices.find_one(record)
		if record: return record['device']
		return None

	def delDevice(self,host,name):
		""" retrieve the device name for an instance on a host """
		record = {
			"node"	: host,
			"name"	: name
		}
		return self.db.devices.remove(record)
	
	def updateDevice(self,host,name,dev):
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
		
		#record = {
		#	"node"		: "node1",
		#	"name"		: "varnish1",
		#	"device"	: "nbd4"
		#}
		#self.db.devices.save(record)
		
		
		
		record = {
			"node"	: host,
			"name"	: name
		}
		return self.db.devices.find_one(record)

	def getNextDevice(self):
		""" find the next free NBD device """
		files = []
		record = {
			"node"	: HOSTNAME
		}
		results = self.db.devices.find(record)
		for result in results:
			path = "/dev/%s" % result['device']
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

	def getInstance(self,host,name):
		""" retrieve the device name for an instance on a host """
		record = {
			"node"	: host,
			"name"	: name
		}
		return self.db.instances.find_one(record)
		
	def getInstances(self,host):
		""" retrieve list of instances for given node """
		record = {
			"node"	: host
		}
		return self.db.instances.find(record)
