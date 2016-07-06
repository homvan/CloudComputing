#!/usr/bin/python
# encoding=UTF-8

import sys
import os
import re


try:
	
	for line in sys.stdin:
		try:
			lineparse = line.strip().split('\t',3)
			userid = int(lineparse[0])
			date = int(lineparse[1])
			score = lineparse[2]
			responce = lineparse[3]
			key ='%010d'%(userid)+'%04d'%(date)
			#print key
			print key+'\t'+score+'\t'+responce
		except:
			continue
except:
	sys.stderr.write(str(sys.exc_info()))
	
