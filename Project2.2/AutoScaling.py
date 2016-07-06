#!/usr/bin/env python

import time
import urllib
import urllib2
import boto
import boto.ec2
import boto.ec2.elb
import boto3
from boto.ec2.connection import EC2Connection
from boto.ec2.elb import ELBConnection
from boto.ec2.elb import HealthCheck
import boto.ec2.autoscale
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import ScalingPolicy
import boto.ec2.cloudwatch
from boto.ec2.cloudwatch import CloudWatchConnection
from boto.ec2.cloudwatch import MetricAlarm

#Connect to AWS
key = open('rootkey.csv','r')
access_key = key.readline().split('=')[1][:-2]
secret_key = key.readline().split('=')[1][:-2]
key.close()


conn = EC2Connection(access_key, secret_key)
elb_conn = ELBConnection(access_key, secret_key)
asg_conn = AutoScaleConnection(access_key, secret_key)
cw_conn = CloudWatchConnection(access_key, secret_key)

LG_IMAGE = 'ami-312b5154'
DC_IMAGE = 'ami-3b2b515e'
KEY_NAME = '15619project22'
LG_TYPE = 'm3.medium'
DC_TYPE = 'm3.large'
ZONE = 'us-east-1c'
PORT = [(80, 80, 'http')]
#SECURITY_GROUP for Load Generator has been created manually in web console
SECURITY_GROUPS_LG = ['Project']
#SECURITY_GROUP for ELB and ASG
SECURITY_GROUP_ELB = conn.create_security_group('All_Traffic', 'All Traffic')
SECURITY_GROUP_ELB.authorize('tcp', 0, 65535, '0.0.0.0/0')
#Submit password
SUBMIT_PASSWORD = '5F0RJU0aurtqkfPbpyYX1Fk2oVo91V9F'

#Check warmup finished
def checkFinished(logURL):
	request = urllib2.urlopen(logURL)
	index = request.read()
	if('finished' in index):
		return True
	else:
		return False

#Check Test End
def checkEnd(logURL):
	request = urllib2.urlopen(logURL)
	index = request.read()
	if('End' in index):
		return True
	else:
		return False

#Create Load Generator

print 'Starting Load Generator of type {0} with image {1}'.format(LG_TYPE, LG_IMAGE)
reservation = conn.run_instances(LG_IMAGE, instance_type = LG_TYPE, key_name = KEY_NAME, placement = ZONE, security_groups = SECURITY_GROUPS_LG)
instance = reservation.instances[0]
time.sleep(10)
while not instance.update() == 'running':
	time.sleep(3)
	
instance.add_tag("Project", "2.2")
LG_DNS = str(instance.dns_name)
print LG_DNS
time.sleep(10)

	
#Create ELB

print 'Creating ELB'
elb = elb_conn.create_load_balancer('Project22hongf', ZONE, PORT)
healthcheck = HealthCheck(access_point = 'Project22hongf', interval=30, healthy_threshold=2, unhealthy_threshold=10, target='HTTP:80/heartbeat?lg='+LG_DNS)
elb.configure_health_check(healthcheck)
ELB_DNS = str(elb.dns_name)
print ELB_DNS
time.sleep(10)

	
#tag ELB

client = boto3.client('elb')
response = client.add_tags(
    LoadBalancerNames=[
        'Project22hongf',
    ],
    Tags=[
        {
            'Key': 'Project',
            'Value': '2.2'
        },
    ]
)
#Create Launch Configuration

print 'Creating LC'
lc = LaunchConfiguration(name='Project22', image_id=DC_IMAGE, key_name=KEY_NAME, instance_type=DC_TYPE, instance_monitoring = True, security_groups=['All_Traffic'])
asg_conn.create_launch_configuration(lc)
print 'LC created'

#Create Auto Scaling Group

print 'Creating ASG'
asg = AutoScalingGroup(group_name='Project22group', load_balancers=['Project22hongf'], health_check_type = 'ELB', health_check_period = '119', desired_capacity = 5, availability_zones=['us-east-1c'], launch_config = lc, min_size = 5, max_size = 5, tags = [boto.ec2.autoscale.tag.Tag(key='Project',value='2.2', resource_id = 'Project22group', propagate_at_launch=True)])
asg_conn.create_auto_scaling_group(asg)
print 'ASG created'

#Create Scaling Policy

print 'Creating Scaling Policy'
scale_out_policy = ScalingPolicy(name = 'scale_out', adjustment_type = 'ChangeInCapacity', as_name = 'Project22group', scaling_adjustment = 1, cooldown = 60)
asg_conn.create_scaling_policy(scale_out_policy)
scale_in_policy = ScalingPolicy(name = 'scale_in', adjustment_type = 'ChangeInCapacity', as_name = 'Project22group', scaling_adjustment = -1, cooldown = 60)
asg_conn.create_scaling_policy(scale_in_policy)

#Check Policys and get them for CloudWatch
ScaleOut = asg_conn.get_all_policies(as_group = 'Project22group', policy_names = ['scale_out'])[0]
ScaleIn = asg_conn.get_all_policies(as_group = 'Project22group', policy_names = ['scale_in'])[0]
print 'Scaling Policy created'

#Create CloudWatch

alarm_dimensions = {"AutoScalingGroupName": 'Project22group'}
scale_out_alarm = MetricAlarm(name = 'scale_out', namespace = 'AWS/EC2', metric = 'CPUUtilization', statistic='Average', comparison='>', threshold='80', period = '60', evaluation_periods=5, alarm_actions=[ScaleOut.policy_arn], dimensions=alarm_dimensions)
cw_conn.create_alarm(scale_out_alarm)
scale_in_alarm = MetricAlarm(name = 'scale_in', namespace = 'AWS/EC2', metric = 'CPUUtilization', statistic='Average', comparison='<', threshold='20', period = '60', evaluation_periods=5, alarm_actions=[ScaleIn.policy_arn], dimensions=alarm_dimensions)
cw_conn.create_alarm(scale_in_alarm)
print'CloudWatch Alarm created'




#wait 200s for instances initiation and then start warm up
print 'Start to wait for instance initiation'
time.sleep(200)

#submit password to LG
print 'submit password to LG'
passwordURL = 'http://'+LG_DNS+'/password?passwd='+SUBMIT_PASSWORD
print passwordURL
request = urllib2.urlopen(passwordURL)
print 'submitting password done'

#Warm up
print 'Start Warming Up'
warmupURL = 'http://'+LG_DNS+'/warmup?dns='+ELB_DNS
print warmupURL
for i in range(1,21):
	print 'Warm up'
	while True:
		try:
			request = urllib2.urlopen(warmupURL)
			print 'warm up request sent'
			break
		except:
			time.sleep(5)
			print 'warmup request send again'
			continue
	tmp = request.read()
	LOG_SUFFIX = tmp.split("'")[1]
	logURL = 'http://'+LG_DNS+LOG_SUFFIX
	while True:
		if(checkFinished(logURL)):
			print 'Warm up NO.'+str(i)+' end'
			break;
		else:
			print 'Warm up NO.'+str(i)+' not end yet'
			time.sleep(30)
			continue
time.sleep(10)

#Change the configuraiton of ASG
asg_client = boto3.client('autoscaling')
response = asg_client.update_auto_scaling_group(
    AutoScalingGroupName='Project22group',
    LaunchConfigurationName='Project22',
    MinSize=2,
    MaxSize=3,
    DesiredCapacity=2,
    DefaultCooldown=60,
    AvailabilityZones=[
        'us-east-1c',
    ],
    HealthCheckType='ELB',
    HealthCheckGracePeriod=119
    
)

#Sleep 60s, wait extra instances terminate
time.sleep(60)

#Start Test
print 'Test Start'
testURL = 'http://'+LG_DNS+'/junior?dns='+ELB_DNS
print 'testURL'
while True:
	try:
		request = urllib2.urlopen(testURL)
		print 'Test Request Sent'
		break
	except:
		time.sleep(5)
		print 'Test Request Send Again'
		continue
tmp = request.read()
LOG_SUFFIX = tmp.split("'")[1]
logURL = 'http://'+LG_DNS+LOG_SUFFIX
while True:
	if(checkEnd(logURL)):
		print 'Test end'
		break;
	else:
		print 'Test not end yet'
		time.sleep(60)
		continue

print 'Terminating resources'
#shut down instances
asg_client = boto3.client('autoscaling')

response = asg_client.update_auto_scaling_group(
    AutoScalingGroupName='Project22group',
    LaunchConfigurationName='Project22',
    MinSize=0,
    MaxSize=0,
    DesiredCapacity=0,
    DefaultCooldown=60,
    AvailabilityZones=[
        'us-east-1c',
    ],
    HealthCheckType='ELB',
    HealthCheckGracePeriod=119
    
)
time.sleep(10)
#delete Auto Scaling Group
asg.delete()
time.sleep(10)
#delete Launch Configuration
lc.delete()
time.sleep(10)
#delete ELB
elb.delete()
time.sleep(10)
#delete security group of elb
SECURITY_GROUP_ELB.delete()
time.sleep(10)

