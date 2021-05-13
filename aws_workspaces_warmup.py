### This script starts your workspaces and should be run as a Lambda Function
### Lambda trigger is a simple CloudWatch cron rule
### IAM rights to run against the workspaces will have to be appropriate for the function

import boto3

$directoryID = 'yourDirectoryID'
$region = 'example us-west-1'

def lambda_handler(event, context):
    directory_id= $directoryID
    region = $region
    running_mode = 'AVAILABLE'
        
    ws = session.client('workspaces')
    workspaces = []
    
    resp = ws.describe_workspaces(DirectoryId=directory_id)
    
    while resp:
      workspaces += resp['Workspaces']
      resp = ws.describe_workspaces(DirectoryId=directory_id, NextToken=resp['NextToken']) if 'NextToken' in resp else None
    
    for workspace in workspaces:
    
      if workspace['State'] == running_mode:
        continue
    
      if workspace['State'] in ['STOPPED']:
    
        ws.start_workspaces(
          StartWorkspaceRequests=[
            {
                'WorkspaceId': workspace['WorkspaceId'],
            },
          ]
        )
      
        print 'Starting WorkSpace for user: ' + workspace['UserName']
    
      else:
        print 'Could not start workspace for user: ' + workspace['UserName']
