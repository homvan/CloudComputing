#!/usr/bin/python
# encoding=UTF-8

import sys

cur_uid = None
followers = ''
for line in sys.stdin:
	try:
		uid, followerid = line.strip().split('\t')
	except:
		continue
	if cur_uid == uid:
		followers = followers + ',' + followerid
	else:
		if cur_uid:
			followers = followers.strip(',')
			print cur_uid+'\t'+followers
		cur_uid = uid;
		followers = followerid

print cur_uid+'\t'+followers.strip(',')
