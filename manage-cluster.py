#!/usr/bin/env python

import os
import argparse
import logging
import sys
import threading
import boto3
import botocore.exceptions
import fabric
import hashlib
import yaml


class EMRClusterManager:
    ACTIONS = ['status', 'start', 'stop', 'notebook', 'ssh']

    def __init__(self, cluster_id=None, aws_profile=None):
        self.cluster_id = cluster_id
        self.boto_session = boto3.Session(profile_name=aws_profile)
        self.emr_client = self.boto_session.client('emr')

    def find_running_clusters(self):
        active_states = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        response = self.emr_client.list_clusters(ClusterStates=active_states)
        return response['Clusters']

    def find_single_cluster(self):
        clusters = self.find_running_clusters()

        if len(clusters) == 0:
            print('No running EMR clusters found. Run \'%s start\' to launch one.' % sys.argv[0])
            return None

        if self.cluster_id:
            cluster = next((c for c in clusters if c['Id'] == self.cluster_id), None)

            if cluster is None:
                print('No running cluster found with ID ' + self.cluster_id)
                return None
        else:
            if len(clusters) > 1:
                cluster_ids = ', '.join([c['Id'] for c in clusters])
                print('Multiple EMR clusters running: %s' % cluster_ids)
                print('You must specify one using --cluster')
                return None

            cluster = clusters[0]

        return cluster

    def status(self):
        clusters = self.find_running_clusters()

        if len(clusters) == 0:
            print('No running EMR clusters found. Run \'%s start\' to launch one.' % sys.argv[0])

        if len(clusters) > 1:
            print('Warning: Multiple EMR clusters running. This is fine, but you will ' + 
                'need to specify the cluster ID using --cluster when running other commands')

        for cluster in clusters:
            response = self.emr_client.list_instance_groups(ClusterId=cluster['Id'])
            group_counts = ['%d %s' % (ig['RunningInstanceCount'], ig['InstanceGroupType']) for ig in response['InstanceGroups']]

            print('* EMR Cluster %s, state: %s' % (cluster['Id'], cluster['Status']['State']))
            print('  - Instances: ' + ', '.join(group_counts))

    def start(self):
        clusters = self.find_running_clusters()

        if len(clusters) > 0:
            cluster_ids = ', '.join([c['Id'] for c in clusters])
            print('EMR cluster(s) already running: %s' % cluster_ids)
            if input('Are you sure you want to launch another one? [y/N] ').lower() != 'y':
                return


        # make sure our version of bootstrap-dask.sh is in S3 already
        s3 = self.boto_session.client('s3')

        bootstrap_script = os.path.join(os.path.dirname(__file__), 'etc/bootstrap-dask.sh')
        environment_file = os.path.join(os.path.dirname(__file__), 'environment.yml')

        with open(bootstrap_script, 'r') as file:
            script_contents = file.read()

        with open(environment_file, 'r') as file:
            environment = yaml.safe_load(file.read())

        script_contents = script_contents.replace(
            '###PACKAGES_FROM_ENVIRONMENT.YML###',
            '\n'.join([dep + ' \\' for dep in environment['dependencies']]))

        sha1_digest = hashlib.sha1(script_contents.encode('utf-8')).hexdigest()

        bootstrap_bucket = 'ai-emr-studio-workspace'  # FIXME
        bootstrap_key = 'bootstrap-dask_%s.sh' % sha1_digest

        try:
            s3.head_object(Bucket=bootstrap_bucket, Key=bootstrap_key)
        except botocore.exceptions.ClientError:
            print('Uploading EMR bootstrap script %s to bucket %s' % (bootstrap_script, bootstrap_bucket))
            s3.put_object(Bucket=bootstrap_bucket, Key=bootstrap_key, Body=script_contents.encode('utf-8'))
        
        response = self.emr_client.run_job_flow(
            Name='AllenInstituteSandbox',
            LogUri='s3n://ai-emr-logs/', # FIXME
            Tags=[{'Key':'for-use-with-amazon-emr-managed-poliies','Value':'true'}],
            ReleaseLabel='emr-6.9.0',
            ServiceRole='arn:aws:iam::265524986479:role/EMRServiceRole', # FIXME
            Applications=[
                {'Name': 'Spark'},
                {'Name': 'Zeppelin'},
            ],
            BootstrapActions=[
                {
                    'Name': 'Bootstrap Dask', 
                    'ScriptBootstrapAction': {
                        'Path': 's3://%s/%s' % (bootstrap_bucket, bootstrap_key)
                    }
                }
            ],
            ManagedScalingPolicy={
                'ComputeLimits': {
                    'UnitType': 'Instances', # FIXME: configurable
                    'MinimumCapacityUnits': 1,
                    'MaximumCapacityUnits': 20,
                    'MaximumOnDemandCapacityUnits': 5,
                    'MaximumCoreCapacityUnits': 5,
                }
            },
            AutoTerminationPolicy={
                'IdleTimeout': 60*60*2  # two hour idle timeout -- FIXME: configurable
            },
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=10,
            JobFlowRole='EMRServiceRoleForEC2',
            Instances={
                'Ec2KeyName': 'Allen Institute Key pair',
                'Ec2SubnetId': 'subnet-0c85321ee3089b282', # FIXME
                'KeepJobFlowAliveWhenNoSteps': True,
                'EmrManagedMasterSecurityGroup': 'sg-03fae5fc06d32d33e', # FIXME
                'EmrManagedSlaveSecurityGroup': 'sg-03fae5fc06d32d33e', # FIXME
                'InstanceGroups': [
                    {
                        'InstanceRole': 'CORE',
                        'Name': 'Core',
                        'InstanceCount': 1,
                        'InstanceType': 'r5.xlarge',
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
                        'InstanceRole': 'MASTER',
                        'Name': 'Primary',
                        'InstanceCount': 1,
                        'InstanceType': 'r5.xlarge',
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
                        'InstanceRole': 'TASK',
                        'Name': 'Task',
                        'InstanceCount': 1,
                        'InstanceType': 'r5.xlarge',
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
                ]
            })

        print('Launched EMR cluster: ' + response['JobFlowId'])
        print('Monitor the startup using \'%s status\'' % sys.argv[0])
        print('Once it has finished bootstrapping, connect using \'%s notebook\' or any script using \'--cluster EMR\'' % sys.argv[0])

    def stop(self):
        cluster = self.find_single_cluster()
        if cluster:
            print('Terminating EMR cluster ' + cluster['Id'])
            self.emr_client.terminate_job_flows(JobFlowIds=[cluster['Id']])

    def ssh(self):
        cluster = self.find_single_cluster()
        if cluster is None:
            return

        response = self.emr_client.describe_cluster(ClusterId=cluster['Id'])
        cluster = response['Cluster']

        private_key = '/Users/danford/Dropbox/Skunkworks/Misc/Allen Institute Key pair.pem' # FIXME
        primary_hostname = cluster['MasterPublicDnsName']
        username = 'hadoop'

        os.system('ssh -i \'%s\' %s@%s' % (private_key, username, primary_hostname))

    def notebook(self):
        cluster = self.find_single_cluster()
        if cluster is None:
            return

        response = self.emr_client.describe_cluster(ClusterId=cluster['Id'])
        cluster = response['Cluster']

        state = cluster['Status']['State']
        notebook_running_states = ['RUNNING', 'WAITING']
        if state not in notebook_running_states:
            print('Cluster %s state is %s, must be one of: %s' % (cluster['Id'], state, notebook_running_states))
            return

        private_key = '/Users/danford/Dropbox/Skunkworks/Misc/Allen Institute Key pair.pem' # FIXME
        primary_hostname = cluster['MasterPublicDnsName']
        username = 'hadoop'
        jupyter_port = 8888

        with fabric.Connection(
            host = primary_hostname,
            user = username,
            connect_kwargs = {
                'key_filename': private_key
            }
        ).forward_local(jupyter_port):
            print('Connected to EMR cluster %s, primary node: %s' % (cluster['Id'], primary_hostname))
            print('Access Jupyter Notebook in your browser at http://localhost:%d' % jupyter_port)
            print('CTRL-C to stop...')
            threading.Event().wait()  # waits forever until CTRL-C


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    arg_parser = argparse.ArgumentParser(prog=sys.argv[0], description=__doc__)
    arg_parser.add_argument('action', help='Action(s) to perform', choices=EMRClusterManager.ACTIONS, nargs='+')
    arg_parser.add_argument('-c', '--cluster', help='Specify EMR cluster ID (e.g., j-ASDF1234)')
    arg_parser.add_argument('-p', '--profile', help='Specify AWS CLI profile to use for credentials (\'aws configure list-profiles\' to list)')
    #arg_parser.add_argument('-i', '--instance-type', help='Specify instance type (default: ')
    args = arg_parser.parse_args()

    cluster_mgr = EMRClusterManager(args.cluster, args.profile)

    for action in args.action:
        getattr(cluster_mgr, action)()
