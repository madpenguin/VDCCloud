#
#	setup.py
#	- VDCCloud (c) Mad Penguin 2012
#
from setuptools import setup
#
setup(
	name = "VDCCloud",
	version = "0.0.1",
	description = ("Virtual Data Centre Cloud Platform"),
	packages = ["VDCCloud"],
	long_description = open("README.md").read(),
	author = "Mad Penguin",
	author_email = "madpenguin@linux.co.uk",
	license = "GPLv3",
	scripts = ["bin/fc_gluster.py"],
	entry_points = { "console_scripts" : [ "fc_gluster = VDCCloud.fc_gluster:run" ] }
	)
