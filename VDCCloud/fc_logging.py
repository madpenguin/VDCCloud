#!/usr/bin/python
#
#	fc_logging - console output routines
#
#	Mad Pengiun <madpenguin@linux.co.uk> (c) 2012
#
#
from termcolor 	import colored
import sys
#
###############################################################################
#
def logBegin(text,name=''):
	""" start logging a line to stdout """
	print colored("*","blue"),
	print colored(text,"green"),
	if name <> '': print colored(name,"yellow"),
	sys.stdout.flush()
#
###############################################################################
#
def logEnd(text,ok):
	""" stop logging and print a status message """
	if ok:
		color = "green"
	else:	color = "red"
	print "["+colored(text,color)+"]"
#
###############################################################################
#
def logMiddle(text,col):
	print "["+colored(text,col)+"]",
	sys.stdout.flush()
#
###############################################################################
#
def logYN(text):
	while True:
		logBegin(text+colored(" (y/N) ",yellow))
		choice = raw_input().lower()
		if not choice in ['y','N','']: continue
		if choice == 'y': return True
		return False

def logError(text,name):
	""" standard format for error display """
	print " "+colored("*","blue"),
	print colored(text,"green"),
	print "["+colored(name,"red")+"]"
#
