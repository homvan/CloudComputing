#!/usr/bin/python
# encoding=UTF-8

import sys
import os 

cur_key = None
cur_value = None
key_suffix = 0
for line in sys.stdin:
	line = line.strip()
	try:
		key,value = line.split('\t',1)
		if cur_key == key:
			key_suffix += 1;
			rowkey = key+'%03d'%(key_suffix)
			print rowkey + '\t' + value
		else:
			cur_key = key
			key_suffix = 0
			rowkey = key+'%03d'%(key_suffix)
			print rowkey + '\t' + value
	except:
		continue
