import json

filename = 'posts.json'
fread = open(filename, 'r')
fwrite = open('postdata', 'w')

for line in fread:
	postline = json.loads(line,"utf-8")
	uid = str(postline['uid'])
	timestamp = postline['timestamp']
	post = line.strip().replace('"', '\\"')
	outputline = 'Uid\x03{"n":"'+uid+'"}\x02Timestamp\x03{"s":"'+timestamp+'"}\x02Post\x03{"s":"'+post+'"}\n'
	fwrite.write(outputline)
