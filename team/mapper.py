#!/usr/bin/python
# encoding=UTF-8

import json
import sys
import os
import re

MONTH = {'Jan':'1','Feb':'2','Mar':'3','Apr':'4','May':'5','Jun':'6','Jul':'7','Aug':'8','Sept':'9','Oct':'10','Nov':'11','Dec':'12'}

filename = "datademo"
f = open(filename, 'r')

#wf = open('result', 'w')

#create timestamp
#check date prior? Sun, 20 Apr 2014 00:00:00 GMT, if yes, ignore
def createTimestamp(time):
	year = time[5]
	month = MONTH[time[1]]
	day = time[2]
	utc = time[3]
	if(year<='2014' and month <='4' and day <= '19'):
		return False
	timestamp = year+'-'+month+'-'+day+'+'+utc
	return timestamp


try:
	#输入文件名可能得改一下
	# get the filename and date
	filename = os.environ["mapreduce_map_input_file"]
	#date=re.search('-(\d{8})-',filename).group(1)
	for line in f:
		try:
			data = json.loads(line,"utf-8")
			time = data['created_at'].split()
			timestamp = createTimestamp(time)
			if(timestamp == False):
				continue
			tweetId = data['id_str']
			Id = data['user']['id_str']
			text = data['text']
			print Id+';'+timestamp+'\t'+tweetId+';'+text+'###'
			#keyval = Id+';'+timestamp+'\t'+tweetId+';'+text+'###\n'
			#wf.write(keyval.encode('utf8'))
		except:
			continue
except:
	#if error then quit
	sys.stderr.write(str(sys.exc_info()))
	sys.stderr.write("fail to read:"+filename+"\n")
