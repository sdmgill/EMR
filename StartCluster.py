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

