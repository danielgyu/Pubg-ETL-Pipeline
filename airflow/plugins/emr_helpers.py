import boto3, time

def run_spark_step(AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Creates a pre-defined EMR cluster that that runs a spark application step.
    The application file is located in line NUM, and the cluster will auto-terminate after completion.

    Params
    ------
    AWS_ACCESS_KEY : str
        access key id for AWS credential
    AWS_SECRET_KEY : str
        secret access key for AWS credential

    Returns
    -------
    cluster_detail : dict
        a dictionary containing different details of the created EMR cluster, such as cluster_id, status, etc
    """

    connection = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    cluster_detail = connection.run_job_flow(
        Name='project pubg',
        LogUri='s3n://aws-logs-599716733371-us-west-2/elasticmapreduce/',
        ReleaseLabel='emr-5.32.0',
        Applications=[
            {
                'Name': 'Spark',
            },
            {
                'Name': 'Hadoop',
            }
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master - 1',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 32
                                },
                                'VolumesPerInstance': 2
                            }
                        ]
                    }
                },
                {
                    'Name': 'Core - 2',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 3,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 32
                                },
                                'VolumesPerInstance': 2
                            }
                        ]
                    }
                }
            ],
            'Ec2KeyName': 'udacity-spark',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-6004d818',
            'EmrManagedMasterSecurityGroup': 'sg-01f3cd9437940abec',
            'EmrManagedSlaveSecurityGroup': 'sg-038b32b1e34d9afd6',
        },
        Steps=[
            {
                'Name': 'processing with spark',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode',
                        'cluster',
                        's3://capstone-pubg/step/etl_pubg.py'
                    ]
                }
            },
        ],
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        EbsRootVolumeSize=10,
        ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION'
    )

    return cluster_detail

def check_emr_cluster(task_instance, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Waits for the cluster, identied by the cluster id, to be terminated.
    Checks on the status every 3 minutes, raises ValueError if cluster was terminated with incompmlete step
    execution, and smoothly ends the function if the cluster was terminated with successful step execution.

    Params
    ------
    AWS_ACCESS_KEY : str
        access key id for AWS credential
    AWS_SECRET_KEY : str
        secret access key for AWS credential

    """
    value = task_instance.xcom_pull(task_ids='emr_step_spark')
    cluster_id = value['JobFlowId']

    connection = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    processing_state = True
    while processing_state:
        print('Waiting for 3 minutes...')
        time.sleep(180)

        response = connection.describe_cluster(ClusterId=cluster_id)
        cluster_state = response['Cluster']['Status']['State']

        if response['Cluster']['Status']['StateChangeReason']['Code'] == 'ALL_STEPS_COMPLETED':
            processing_state = False
            pritn('EMR step complete')
        elif cluster_state == 'TERMINATED_WITH_ERRORS':
            raise ValueError("CLUSTER FAILURE REASON : {}".format(state_code))
        print('End of iteration, cluster_state = {}'.format(cluster_state))
