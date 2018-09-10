import boto3

client = boto3.client('emr')

response=client.list_clusters(ClusterStates=['WAITING','RUNNING','BOOTSTRAPPING','STARTING'])

response
#for clusters in response:
#    for id in response[clusters]:
#        print(id ['Id'])