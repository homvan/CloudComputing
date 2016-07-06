#!/usr/bin/python
# encoding=UTF-8

import sys
import os
import re


for line in sys.stdin:
	try:
		line = line.strip()
		splits = line.split(',')
		print splits[0]+'\t'+splits[1]
	except:
		sys.stderr.write("error line")

