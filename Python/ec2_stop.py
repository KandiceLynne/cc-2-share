## Author - Kandice Hendricks
## Version 1.0
## Date 05/13/2021
## This is a very basic PowerShell Script that will do a compare of your input file
## Put this in a Lambda function that is triggered by a CloudWatch scheduled rule
## This will start all EC2 instances with the tag name of AutoStop and a value of Yes
## All tag names and values ARE case sensitive



import boto3
import logging

#setup simple logging for INFO
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#define the connection
ec2 = boto3.resource('ec2')

def lambda_handler(event, context):
    # Use the filter() method of the instances collection to retrieve
    # all running EC2 instances.
    filters = [{
            'Name': 'tag:AutoStop',
            'Values': ['Yes']
        },
        {
            'Name': 'instance-state-name', 
            'Values': ['running']
        }
    ]
    
    #filter the instances
    instances = ec2.instances.filter(Filters=filters)

    #locate all running instances
    RunningInstances = [instance.id for instance in instances]
    
    print('the instances for logging purposes')
    print('RunningInstances') 
    
    #make sure there are actually instances to shut down. 
    if len(RunningInstances) > 0:
        #perform the shutdown
        shuttingDown = ec2.instances.filter(InstanceIds=RunningInstances).stop()
        print('shuttingDown')
    else:
        print("Nothing to see here")
