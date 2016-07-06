#!/usr/bin/env python

import os
from boto.ec2.connection import EC2Connection
import time
import urllib2

#initiator
LG_IMAGE = 'ami-4389fb26'
DC_IMAGE = 'ami-abb8cace'
KEY_NAME = '15619hongf2.1'
INSTANCE_TYPE = 'm3.medium'
ZONE = 'us-east-1b'
SECURITY_GROUPS = ['Project2.1']
LG_DNS = ''
DC_DNS = ''
LOG_SUFFIX = ''
#Submit password
SUBMIT_PASSWORD = '5F0RJU0aurtqkfPbpyYX1Fk2oVo91V9F'

#Connect to AWS\
key = open('rootkey.csv','r')
access_key = key.readline().split('=')[1][:-2]
secret_key = key.readline().split('=')[1][:-2]
key.close()
conn = EC2Connection(access_key, secret_key)



#Create Load Generator
def createLG():
	
	print 'Starting Load Generator of type {0} with image {1}'.format(INSTANCE_TYPE, LG_IMAGE)
	reservation = conn.run_instances(LG_IMAGE, instance_type = INSTANCE_TYPE, key_name = KEY_NAME, placement = ZONE, security_groups = SECURITY_GROUPS)
	instance = reservation.instances[0]
	time.sleep(10)
	
	while not instance.update() == 'running':
		time.sleep(3)
		
	instance.add_tag("Project", "2.1")
	LG_DNS = str(instance.dns_name)
	print LG_DNS
	time.sleep(10)
	return LG_DNS
	
	
	
	
def createDC():
	
	print 'Starting Data Center of type {0} with image {1}'.format(INSTANCE_TYPE, DC_IMAGE)
	reservation = conn.run_instances(DC_IMAGE, instance_type = INSTANCE_TYPE, key_name = KEY_NAME, placement = ZONE, security_groups = SECURITY_GROUPS)
	instance = reservation.instances[0]
	time.sleep(10)
	
	while not instance.update() == 'running':
		time.sleep(3)
		
	
	instance.add_tag("Project", "2.1")
	DC_DNS = str(instance.dns_name)
	print DC_DNS
	time.sleep(10)
	return DC_DNS
	
def getRPS(logURL):
	request = urllib2.urlopen(logURL)
	rps = 0
	for line in request:
		if 'Minute' in line:
			rps = 0
		if 'compute' in line:
			parse = line.split('=')[1][:-1]
			rps += float(parse)
	print 'current rps is %f' % rps	
	return rps

LG_DNS = createLG()
DC_DNS = createDC()
time.sleep(100)

#submit password to LG
print 'submit password to LG'
passwordURL = 'http://'+LG_DNS+'/password?passwd='+SUBMIT_PASSWORD
print passwordURL
request = urllib2.urlopen(passwordURL)
print 'submitting password done'

#test start
print 'test start'
testURL = 'http://'+LG_DNS+'/test/horizontal?dns='+DC_DNS
request = urllib2.urlopen(testURL)

#grap log url
tmp = request.read()
LOG_SUFFIX = tmp.split("'")[1]
logURL = 'http://'+LG_DNS+LOG_SUFFIX
print logURL

time.sleep(100)


#get RPS

cur_rps = getRPS(logURL)


#add DC till rps >= 4000
while cur_rps < 4000:
	print 'add new DC'
	new_DC_DNS = createDC()
	time.sleep(100)
	#url to add new DC	
	addURL = 'http://'+LG_DNS+'/test/horizontal/add?dns='+new_DC_DNS
	print 'add new DC'+new_DC_DNS+'to LG'
	request = urllib2.urlopen(addURL)
	time.sleep(100)
	cur_rps = getRPS(logURL)
	










