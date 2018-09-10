import boto3

client = boto3.client('emr')

response=client.list_clusters(ClusterStates=['WAITING','RUNNING','BOOTSTRAPPING','STARTING'])


#terminate = response['Clusters'][0]['Id']
#print(terminate)
response = client.terminate_job_flows(JobFlowIds=['j-1WN2J9KUPNRLM'])