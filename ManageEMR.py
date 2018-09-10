import boto3
import time

client = boto3.client('emr')


def build_emr_cluster(env):
    if str.upper(env) == 'DEV':
        cluster = client.run_job_flow(
            Name='Spark-m4large-bootstrapped-configjson-boto3',
            ReleaseLabel='emr-5.13.0',
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}, {'Name': 'Zeppelin'}, {'Name': 'Ganglia'}],
            Instances={
                'InstanceGroups': [{'Name': 'Master - 1',
                                    'InstanceCount': 1,
                                    'EbsConfiguration':
                                        {'EbsBlockDeviceConfigs': [
                                            {'VolumeSpecification':
                                                {
                                                    'SizeInGB': 32,
                                                    'VolumeType': 'gp2'
                                                },
                                                'VolumesPerInstance': 1
                                            }
                                        ]}, 'InstanceRole': 'MASTER',
                                    'InstanceType': 'm4.large'
                                    },
                                   {'Name': 'Core - 2',
                                    'Market': 'SPOT',
                                    'InstanceCount': 2,
                                    # 'BidPrice': 'OnDemandPrice',
                                    "AutoScalingPolicy": {"Constraints": {
                                        "MinCapacity": 2,
                                        "MaxCapacity": 10
                                    },
                                        "Rules": [
                                            {
                                                "Name": "LowYarnMem",
                                                "Description": "",
                                                "Action": {
                                                    "SimpleScalingPolicyConfiguration": {
                                                        "ScalingAdjustment": 1,
                                                        "CoolDown": 300,
                                                        "AdjustmentType": "CHANGE_IN_CAPACITY"
                                                    }
                                                },
                                                "Trigger": {
                                                    "CloudWatchAlarmDefinition": {
                                                        "MetricName": "YARNMemoryAvailablePercentage",
                                                        "ComparisonOperator": "LESS_THAN_OR_EQUAL",
                                                        "Statistic": "AVERAGE",
                                                        "Period": 300,
                                                        "Dimensions": [
                                                            {
                                                                "Value": "${emr.clusterId}",
                                                                "Key": "JobFlowId"
                                                            }
                                                        ],
                                                        "EvaluationPeriods": 1,
                                                        "Unit": "PERCENT",
                                                        "Namespace": "AWS/ElasticMapReduce",
                                                        "Threshold": 10
                                                    }
                                                },
                                            },
                                            {
                                                "Name": "LowAvailMem",
                                                "Description": "",
                                                "Action": {
                                                    "SimpleScalingPolicyConfiguration": {
                                                        "ScalingAdjustment": 1,
                                                        "CoolDown": 300,
                                                        "AdjustmentType": "CHANGE_IN_CAPACITY"
                                                    }
                                                },
                                                "Trigger": {
                                                    "CloudWatchAlarmDefinition": {
                                                        "MetricName": "MemoryAvailableMB",
                                                        "ComparisonOperator": "LESS_THAN_OR_EQUAL",
                                                        "Statistic": "AVERAGE", "Period": 300,
                                                        "Dimensions": [
                                                            {
                                                                "Value": "${emr.clusterId}",
                                                                "Key": "JobFlowId"
                                                            }
                                                        ],
                                                        "EvaluationPeriods": 1,
                                                        "Unit": "COUNT",
                                                        "Namespace": "AWS/ElasticMapReduce",
                                                        "Threshold": 512
                                                    }
                                                },
                                            },
                                            {
                                                "Name": "LowHDFSSpace",
                                                "Description": "",
                                                "Action": {
                                                    "SimpleScalingPolicyConfiguration": {
                                                        "ScalingAdjustment": 1,
                                                        "CoolDown": 300,
                                                        "AdjustmentType": "CHANGE_IN_CAPACITY"
                                                    }
                                                },
                                                "Trigger": {
                                                    "CloudWatchAlarmDefinition": {
                                                        "MetricName": "HDFSUtilization",
                                                        "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                                                        "Statistic": "AVERAGE",
                                                        "Period": 300,
                                                        "Dimensions": [
                                                            {
                                                                "Value": "${emr.clusterId}",
                                                                "Key": "JobFlowId"
                                                            }
                                                        ],
                                                        "EvaluationPeriods": 1,
                                                        "Unit": "PERCENT",
                                                        "Namespace": "AWS/ElasticMapReduce",
                                                        "Threshold": 80
                                                    }
                                                },
                                            },
                                            {
                                                "Name": "HighYarnMem",
                                                "Description": "",
                                                "Action": {
                                                    "SimpleScalingPolicyConfiguration": {
                                                        "ScalingAdjustment": -1,
                                                        "CoolDown": 300,
                                                        "AdjustmentType": "CHANGE_IN_CAPACITY"
                                                    }
                                                },
                                                "Trigger": {
                                                    "CloudWatchAlarmDefinition": {
                                                        "MetricName": "YARNMemoryAvailablePercentage",
                                                        "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                                                        "Statistic": "AVERAGE",
                                                        "Period": 300,
                                                        "Dimensions": [
                                                            {
                                                                "Value": "${emr.clusterId}",
                                                                "Key": "JobFlowId"
                                                            }
                                                        ],
                                                        "EvaluationPeriods": 1,
                                                        "Unit": "PERCENT",
                                                        "Namespace": "AWS/ElasticMapReduce",
                                                        "Threshold": 80
                                                    }
                                                },
                                            },
                                            {
                                                "Name": "Idle",
                                                "Description": "",
                                                "Action": {
                                                    "SimpleScalingPolicyConfiguration": {
                                                        "ScalingAdjustment": -1,
                                                        "CoolDown": 300,
                                                        "AdjustmentType": "CHANGE_IN_CAPACITY"
                                                    }
                                                },
                                                "Trigger": {
                                                    "CloudWatchAlarmDefinition": {
                                                        "MetricName": "IsIdle",
                                                        "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                                                        "Statistic": "AVERAGE",
                                                        "Period": 300,
                                                        "Dimensions": [
                                                            {"Value": "${emr.clusterId}",
                                                             "Key": "JobFlowId"
                                                             }
                                                        ],
                                                        "EvaluationPeriods": 2,
                                                        "Unit": "NONE",
                                                        "Namespace": "AWS/ElasticMapReduce",
                                                        "Threshold": 1
                                                    }
                                                },
                                            }
                                        ]
                                    },
                                    'EbsConfiguration':
                                        {'EbsBlockDeviceConfigs': [
                                            {'VolumeSpecification':
                                                {
                                                    'SizeInGB': 32,
                                                    'VolumeType': 'gp2'
                                                },
                                                'VolumesPerInstance': 1
                                            }
                                        ]}, 'InstanceRole': 'CORE',
                                    'InstanceType': 'm4.large'
                                    },
                                   ],
                'Ec2KeyName': 'bi-emr',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-2d825865',
                'EmrManagedMasterSecurityGroup': 'sg-db8f87a7',
                'EmrManagedSlaveSecurityGroup': 'sg-b38088cf',
                'ServiceAccessSecurityGroup': 'sg-b28d85ce'
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            EbsRootVolumeSize=20,
            AutoScalingRole='EMR_AutoScaling_DefaultRole',
            LogUri='s3n://aws-logs-062519970039-us-west-2/elasticmapreduce/',
            BootstrapActions=[{'Name': 'Custom action',
                               'ScriptBootstrapAction': {
                                   'Path': 's3://pa-emr-bootstrap-files/spark/spark-bootstrap-v2.sh'}
                               }],
            Configurations=[{'Classification': 'spark-defaults',
                             'Properties': {
                                 'spark.executor.memory': '2G',
                                 'spark.driver.memory': '2G',
                                 'spark.driver.cores': '2',
                                 'spark.driver.maxResultSize': '1G'
                             },
                             'Configurations': []},
                            {
                                'Classification': 'spark-hive-site',
                                'Properties': {
                                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                                },
                                'Configurations': []
                            }
                            ]

        )
        return(cluster['JobFlowId'])
    elif str.upper(env) in ('UAT','TEST','PROD'):
        cluster = client.run_job_flow(
            Name='Spark-m52xlarge-bootstrapped-configjson-boto3',
            ReleaseLabel='emr-5.13.0',
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}, {'Name': 'Zeppelin'}, {'Name': 'Ganglia'}],
            Instances={
                'InstanceGroups': [{'Name': 'Master - 1',
                                    'InstanceCount': 1,
                                    'EbsConfiguration':
                                        {'EbsBlockDeviceConfigs': [
                                            {'VolumeSpecification':
                                                {
                                                    'SizeInGB': 32,
                                                    'VolumeType': 'gp2'
                                                },
                                                'VolumesPerInstance': 1
                                            }
                                        ]}, 'InstanceRole': 'MASTER',
                                    'InstanceType': 'm5.2xlarge'
                                    },
                                   {'Name': 'Core - 2',
                                    'Market': 'SPOT',
                                    'InstanceCount': 2,
                                    # 'BidPrice': 'OnDemandPrice',
                                    "AutoScalingPolicy": {"Constraints":{
                                                                        "MinCapacity":2,
                                                                        "MaxCapacity":10
                                                                        },
                                                        "Rules":[
                                                            {
                                                            "Name":"LowYarnMem",
                                                            "Description":"",
                                                            "Action":{
                                                                        "SimpleScalingPolicyConfiguration":{
                                                                                                            "ScalingAdjustment":1,
                                                                                                            "CoolDown":300,
                                                                                                            "AdjustmentType":"CHANGE_IN_CAPACITY"
                                                                                                            }
                                                                                                        },
                                                            "Trigger":{
                                                                        "CloudWatchAlarmDefinition":{
                                                                                                    "MetricName":"YARNMemoryAvailablePercentage",
                                                                                                    "ComparisonOperator":"LESS_THAN_OR_EQUAL",
                                                                                                    "Statistic":"AVERAGE",
                                                                                                    "Period":300,
                                                                                                    "Dimensions":[
                                                                                                                {
                                                                                                                "Value":"${emr.clusterId}",
                                                                                                                "Key":"JobFlowId"
                                                                                                                }
                                                                                                                ],
                                                                                                    "EvaluationPeriods":1,
                                                                                                    "Unit":"PERCENT",
                                                                                                    "Namespace":"AWS/ElasticMapReduce",
                                                                                                    "Threshold":10
                                                                                                    }
                                                                        },
                                                            },
                                                            {
                                                            "Name":"LowAvailMem",
                                                            "Description":"",
                                                            "Action":{
                                                                        "SimpleScalingPolicyConfiguration":{
                                                                                                            "ScalingAdjustment":1,
                                                                                                            "CoolDown":300,
                                                                                                            "AdjustmentType":"CHANGE_IN_CAPACITY"
                                                                                                            }
                                                                    },
                                                            "Trigger":{
                                                                        "CloudWatchAlarmDefinition":{
                                                                                                    "MetricName":"MemoryAvailableMB",
                                                                                                    "ComparisonOperator":"LESS_THAN_OR_EQUAL",
                                                                                                    "Statistic":"AVERAGE","Period":300,
                                                                                                    "Dimensions":[
                                                                                                                {
                                                                                                                "Value":"${emr.clusterId}",
                                                                                                                "Key":"JobFlowId"
                                                                                                                }
                                                                                                                ],
                                                                                                    "EvaluationPeriods":1,
                                                                                                    "Unit":"COUNT",
                                                                                                    "Namespace":"AWS/ElasticMapReduce",
                                                                                                    "Threshold":512
                                                                                                    }
                                                                        },
                                                            },
                                                            {
                                                            "Name":"LowHDFSSpace",
                                                            "Description":"",
                                                            "Action":{
                                                                        "SimpleScalingPolicyConfiguration":{
                                                                                                            "ScalingAdjustment":1,
                                                                                                            "CoolDown":300,
                                                                                                            "AdjustmentType":"CHANGE_IN_CAPACITY"
                                                                                                            }
                                                                        },
                                                            "Trigger":{
                                                                        "CloudWatchAlarmDefinition":{
                                                                                                    "MetricName":"HDFSUtilization",
                                                                                                    "ComparisonOperator":"GREATER_THAN_OR_EQUAL",
                                                                                                    "Statistic":"AVERAGE",
                                                                                                    "Period":300,
                                                                                                    "Dimensions":[
                                                                                                                {
                                                                                                                "Value":"${emr.clusterId}",
                                                                                                                "Key":"JobFlowId"
                                                                                                                }
                                                                                                                ],
                                                                                                    "EvaluationPeriods":1,
                                                                                                    "Unit":"PERCENT",
                                                                                                    "Namespace":"AWS/ElasticMapReduce",
                                                                                                    "Threshold":80
                                                                                                    }
                                                                        },
                                                            },
                                                            {
                                                            "Name":"HighYarnMem",
                                                            "Description":"",
                                                            "Action":{
                                                                        "SimpleScalingPolicyConfiguration":{
                                                                                                            "ScalingAdjustment":-1,
                                                                                                            "CoolDown":300,
                                                                                                            "AdjustmentType":"CHANGE_IN_CAPACITY"
                                                                                                            }
                                                                        },
                                                            "Trigger":{
                                                                        "CloudWatchAlarmDefinition":{
                                                                                                    "MetricName":"YARNMemoryAvailablePercentage",
                                                                                                    "ComparisonOperator":"GREATER_THAN_OR_EQUAL",
                                                                                                    "Statistic":"AVERAGE",
                                                                                                    "Period":300,
                                                                                                    "Dimensions":[
                                                                                                                {
                                                                                                                "Value":"${emr.clusterId}",
                                                                                                                "Key":"JobFlowId"
                                                                                                                }
                                                                                                                ],
                                                                                                    "EvaluationPeriods":1,
                                                                                                    "Unit":"PERCENT",
                                                                                                    "Namespace":"AWS/ElasticMapReduce",
                                                                                                    "Threshold":80
                                                                                                    }
                                                                        },
                                                            },
                                                            {
                                                            "Name":"Idle",
                                                            "Description":"",
                                                            "Action":{
                                                                        "SimpleScalingPolicyConfiguration":{
                                                                                                            "ScalingAdjustment":-1,
                                                                                                            "CoolDown":300,
                                                                                                            "AdjustmentType":"CHANGE_IN_CAPACITY"
                                                                                                            }
                                                                        },
                                                            "Trigger":{
                                                                        "CloudWatchAlarmDefinition":{
                                                                                                    "MetricName":"IsIdle",
                                                                                                    "ComparisonOperator":"GREATER_THAN_OR_EQUAL",
                                                                                                    "Statistic":"AVERAGE",
                                                                                                    "Period":300,
                                                                                                    "Dimensions":[
                                                                                                                {"Value":"${emr.clusterId}",
                                                                                                                "Key":"JobFlowId"
                                                                                                                }
                                                                                                                ],
                                                                                                    "EvaluationPeriods":2,
                                                                                                    "Unit":"NONE",
                                                                                                    "Namespace":"AWS/ElasticMapReduce",
                                                                                                    "Threshold":1
                                                                                                    }
                                                                        },
                                                            }
                                                            ]
                                                        },
                                    'EbsConfiguration':
                                        {'EbsBlockDeviceConfigs': [
                                            {'VolumeSpecification':
                                                {
                                                    'SizeInGB': 32,
                                                    'VolumeType': 'gp2'
                                                },
                                                'VolumesPerInstance': 1
                                            }
                                        ]}, 'InstanceRole': 'CORE',
                                    'InstanceType': 'm5.2xlarge'
                                    },
                                   ],
                'Ec2KeyName': 'bi-emr',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-2d825865',
                'EmrManagedMasterSecurityGroup': 'sg-db8f87a7',
                'EmrManagedSlaveSecurityGroup': 'sg-b38088cf',
                'ServiceAccessSecurityGroup': 'sg-b28d85ce'
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            EbsRootVolumeSize=20,
            AutoScalingRole='EMR_AutoScaling_DefaultRole',
            LogUri='s3n://aws-logs-062519970039-us-west-2/elasticmapreduce/',
            BootstrapActions=[{'Name': 'Custom action',
                               'ScriptBootstrapAction': {
                                   'Path': 's3://pa-emr-bootstrap-files/spark/spark-bootstrap-v2.sh'}
                               }],
            Configurations=[{'Classification': 'spark-defaults',
                             'Properties': {
                                 'spark.executor.memory': '15G',
                                 'spark.driver.memory': '15G',
                                 'spark.driver.cores': '8',
                                 'spark.driver.maxResultSize': '10G'
                             },
                             'Configurations': []},
                            {
                                'Classification': 'spark-hive-site',
                                'Properties': {
                                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                                },
                                'Configurations': []
                            }
                            ]

        )
        return(cluster['JobFlowId'])
    else:
        print("Invalid Environment. Must pass 'DEV','TEST', 'UAT' or 'PROD'")



def cluster_status(clusterid):
    notready = True
    while notready:
        clusinfo = client.describe_cluster(ClusterId=clusterid)
        status = clusinfo['Cluster']['Status']['State']

        if status == 'WAITING':
            print(status)
            notready = False

        else:
            print(status)
            time.sleep(120) # sleep 2 minutes

def list_running_clusters():
    response = client.list_clusters(ClusterStates=['WAITING', 'RUNNING', 'BOOTSTRAPPING', 'STARTING'])

    clusters = {}
    clustlist = []
    for clid in response.values():
        for clid2 in clid:
            try:
                clusters.update({'Id': clid2['Id'], 'Name': clid2['Name'], 'State': clid2['Status']['State'],
                                 'NormalizedInstanceHours': clid2['NormalizedInstanceHours']})
                clustlist.append(clusters)
            except TypeError:
                pass

    print(clustlist)

def terminate_running_clusters(clusterid=None):
    if clusterid is None:
        response = client.list_clusters(ClusterStates=['WAITING', 'RUNNING', 'BOOTSTRAPPING', 'STARTING'])

        clusters = []
        for clid in response.values():
            for clid2 in clid:
                try:
                    clusters.append(clid2['Id'])
                except TypeError:
                    pass

        for value in clusters:
            client.terminate_job_flows(JobFlowIds=[value])
    else:
        client.terminate_job_flows(JobFlowIds=[clusterid])


