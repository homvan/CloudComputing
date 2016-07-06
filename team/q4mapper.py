#!/usr/bin/python
# encoding=UTF-8

import sys
import os

try:
	for line in sys.stdin:
		try:
			lineparse = line.strip().split('\t',3)
			hashtag = lineparse[0]
			count = lineparse[1]
			date = lineparse[2]
			responce = lineparse[3]
			print hashtag+'\t'+count+'\t'+responce
		except:
			continue

except:
	sys.stderr.write(str(sys.exc_info()))
