import boto3

client = boto3.client('emr')

response=client.list_clusters(ClusterStates=['WAITING','RUNNING','BOOTSTRAPPING','STARTING'])

clusters = [response]
#print(clusters[0])
#print(len(clusters[0]))
print(response)
print(response['Clusters'][0]['Name'],response['Clusters'][0]['Id'],response['Clusters'][0]['Status']['State'])
#print(response['Clusters'][0]['Status']['State'])

