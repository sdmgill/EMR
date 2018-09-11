import boto3


client = boto3.client('emr')


def Start_PositionEnh_PDS_N4_EMR_JobFlow():
    cluster = client.run_job_flow(
        Name='Spark-r5xlarge-PositionEnh-PDS-N4-Blending',
        ReleaseLabel='emr-5.16.0',
        Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}, {'Name': 'Zeppelin'}, {'Name': 'Ganglia'}],
        Instances={
            'InstanceGroups': [{'Name': 'Master',
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
                                'InstanceType': 'r5.xlarge'
                                },
                               {'Name': 'Core',
                                'Market': 'SPOT',
                                'InstanceCount': 4,
                                # 'BidPrice': 'OnDemandPrice',
                                'AutoScalingPolicy': {'Constraints': {
                                    'MinCapacity': 4,
                                    'MaxCapacity': 10
                                },
                                    'Rules': [
                                        {
                                            'Name': 'LowYarnMem',
                                            'Description': '',
                                            'Action': {
                                                'SimpleScalingPolicyConfiguration': {
                                                    'ScalingAdjustment': 1,
                                                    'CoolDown': 300,
                                                    'AdjustmentType': 'CHANGE_IN_CAPACITY'
                                                }
                                            },
                                            'Trigger': {
                                                'CloudWatchAlarmDefinition': {
                                                    'MetricName': 'YARNMemoryAvailablePercentage',
                                                    'ComparisonOperator': 'LESS_THAN_OR_EQUAL',
                                                    'Statistic': 'AVERAGE',
                                                    'Period': 300,
                                                    'Dimensions': [
                                                        {
                                                            'Value': '${emr.clusterId}',
                                                            'Key': 'JobFlowId'
                                                        }
                                                    ],
                                                    'EvaluationPeriods': 1,
                                                    'Unit': 'PERCENT',
                                                    'Namespace': 'AWS/ElasticMapReduce',
                                                    'Threshold': 10
                                                }
                                            },
                                        },
                                        {
                                            'Name': 'LowAvailMem',
                                            'Description': '',
                                            'Action': {
                                                'SimpleScalingPolicyConfiguration': {
                                                    'ScalingAdjustment': 1,
                                                    'CoolDown': 300,
                                                    'AdjustmentType': 'CHANGE_IN_CAPACITY'
                                                }
                                            },
                                            'Trigger': {
                                                'CloudWatchAlarmDefinition': {
                                                    'MetricName': 'MemoryAvailableMB',
                                                    'ComparisonOperator': 'LESS_THAN_OR_EQUAL',
                                                    'Statistic': 'AVERAGE', 'Period': 300,
                                                    'Dimensions': [
                                                        {
                                                            'Value': '${emr.clusterId}',
                                                            'Key': 'JobFlowId'
                                                        }
                                                    ],
                                                    'EvaluationPeriods': 1,
                                                    'Unit': 'COUNT',
                                                    'Namespace': 'AWS/ElasticMapReduce',
                                                    'Threshold': 512
                                                }
                                            },
                                        },
                                        {
                                            'Name': 'LowHDFSSpace',
                                            'Description': '',
                                            'Action': {
                                                'SimpleScalingPolicyConfiguration': {
                                                    'ScalingAdjustment': 1,
                                                    'CoolDown': 300,
                                                    'AdjustmentType': 'CHANGE_IN_CAPACITY'
                                                }
                                            },
                                            'Trigger': {
                                                'CloudWatchAlarmDefinition': {
                                                    'MetricName': 'HDFSUtilization',
                                                    'ComparisonOperator': 'GREATER_THAN_OR_EQUAL',
                                                    'Statistic': 'AVERAGE',
                                                    'Period': 300,
                                                    'Dimensions': [
                                                        {
                                                            'Value': '${emr.clusterId}',
                                                            'Key': 'JobFlowId'
                                                        }
                                                    ],
                                                    'EvaluationPeriods': 1,
                                                    'Unit': 'PERCENT',
                                                    'Namespace': 'AWS/ElasticMapReduce',
                                                    'Threshold': 80
                                                }
                                            },
                                        },
                                        {
                                            'Name': 'HighYarnMem',
                                            'Description': '',
                                            'Action': {
                                                'SimpleScalingPolicyConfiguration': {
                                                    'ScalingAdjustment': -1,
                                                    'CoolDown': 300,
                                                    'AdjustmentType': 'CHANGE_IN_CAPACITY'
                                                }
                                            },
                                            'Trigger': {
                                                'CloudWatchAlarmDefinition': {
                                                    'MetricName': 'YARNMemoryAvailablePercentage',
                                                    'ComparisonOperator': 'GREATER_THAN_OR_EQUAL',
                                                    'Statistic': 'AVERAGE',
                                                    'Period': 300,
                                                    'Dimensions': [
                                                        {
                                                            'Value': '${emr.clusterId}',
                                                            'Key': 'JobFlowId'
                                                        }
                                                    ],
                                                    'EvaluationPeriods': 1,
                                                    'Unit': 'PERCENT',
                                                    'Namespace': 'AWS/ElasticMapReduce',
                                                    'Threshold': 80
                                                }
                                            },
                                        },
                                        {
                                            'Name': 'Idle',
                                            'Description': '',
                                            'Action': {
                                                'SimpleScalingPolicyConfiguration': {
                                                    'ScalingAdjustment': -1,
                                                    'CoolDown': 300,
                                                    'AdjustmentType': 'CHANGE_IN_CAPACITY'
                                                }
                                            },
                                            'Trigger': {
                                                'CloudWatchAlarmDefinition': {
                                                    'MetricName': 'IsIdle',
                                                    'ComparisonOperator': 'GREATER_THAN_OR_EQUAL',
                                                    'Statistic': 'AVERAGE',
                                                    'Period': 300,
                                                    'Dimensions': [
                                                        {'Value': '${emr.clusterId}',
                                                         'Key': 'JobFlowId'
                                                         }
                                                    ],
                                                    'EvaluationPeriods': 2,
                                                    'Unit': 'NONE',
                                                    'Namespace': 'AWS/ElasticMapReduce',
                                                    'Threshold': 1
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
                                'InstanceType': 'r5.xlarge'
                                },
                               ],
            'Ec2KeyName': 'bi-emr',
            'KeepJobFlowAliveWhenNoSteps': False,
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
        BootstrapActions=[{'Name': 'Import Python Modules',
                           'ScriptBootstrapAction': {
                               'Path': 's3://pa-emr-bootstrap-files/spark/spark-bootstrap-v2.sh'}
                           }],
        Configurations=[{'Classification': 'spark-defaults',
                         'Properties': {
                                'spark.executor.memory':'9G',
                                'spark.driver.memory':'15G',
                                'spark.driver.cores':'4',
                                'spark.driver.maxResultSize':'15G',
                                'spark.dynamicAllocation.minExecutors':'3',
                                'spark.executor.cores':'4',
                                'spark.shuffle.service.enabled':'true',
                                'spark.dynamicAllocation.initialExecutors':'3',
                                'spark.dynamicAllocation.enabled':'true'
                         },
                         'Configurations': []
                         },
    {
                            'Classification':'spark-env',
                            'Properties':{},
                            'Configurations':[
                                                {
                                                    'Classification':'export',
                                                    'Properties':{
                                                                    'EXTRA_CLASSPATH':'s3://pa-emr-build-files/jar/harsha2010:magellan-1.0.5-s_2.11.jar'
                                                                },
                                                    'Configurations':[]
                                                }
                                                ]
                        },

                        {
                            'Classification': 'spark-hive-site',
                            'Properties': {
                                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                            },
                            'Configurations': []
                        }
                        ],
        Steps=[
                {
                    'Name':'PositionLot',
                    'ActionOnFailure':'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                                'Args':[
                                            'spark-submit',
                                            '--deploy-mode','cluster',
                                            '--packages','harsha2010:magellan:1.0.5-s_2.11',
                                            '--class','pa.rep',
                                            's3://pa-emr-build-files/getLotsInformation/pa-0.0.1-SNAPSHOT.jar'
                                        ],
                                'Jar':'command-runner.jar',
                                #'MainClass':'pa.rep',
                                #'Properties':[{}]
                                }
                },
                {
                    'Name':'PDSDataExtraction',
                    'ActionOnFailure':'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                                'Args':[
                                            'spark-submit',
                                            '--deploy-mode','cluster',
                                            's3://pa-emr-build-files/pyspark-jobs/pdsdataextraction/getFlatFile.py'
                                        ],
                                'Jar':'command-runner.jar'
                                #'Properties':'',
                             }
                }
            ]

    )

Start_PositionEnh_PDS_N4_EMR_JobFlow()