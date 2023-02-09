"""Module to assist in creating and connecting to a Dask cluster in scripts.

This module exports a single Python class: AutoDaskCluster. The best way to use
it is as a Python context manager ('with' statement) so that it can automatically
clean up any remote resources used by your script:

with AutoDaskCluster() as cluster, dask.distributed.Client(cluster) as client:
    # now you can do Dask operations in here and the cluster will be
    # automatically cleaned up after the block exits.


By default, it creates a local Dask cluster (multiple Python processes running
on your local machine). However, if you specify the EMR cluster type, it will
automatically connect to your currently running EMR cluster, create a new
Dask YarnCluster on it, and create an SSH tunnel so your Dask client can talk
to the cluster (and you can monitor the Dask dashboard):

with AutoDaskCluster('EMR') as cluster, dask.distributed.Client(cluster) as client:
    # Inside this block, a Dask cluster is created on the running EMR cluster.
    # Once the block exits, the Dask cluster (but NOT the EMR cluster) is shut down.

Keep in mind when running on the EMR cluster, you should try to load all your inputs
and write all your outputs using Dask to AWS or GCP cloud storage. You want Dask
workers to be pulling raw data directly to/from cloud storage, rather than streaming
data through the SSH tunnel you have open to the EMR cluster.
"""

import os
import time
import re
import boto3
import fabric
import dask
import dask.distributed


class AutoDaskCluster:

    class ClusterType:
        LOCAL = 'LOCAL'
        EMR = 'EMR'

    @classmethod
    def add_argparse_args(cls, parser):
        parser.add_argument('-c', '--cluster', default=AutoDaskCluster.ClusterType.LOCAL,
            help=('Specify a Dask cluster to use (\'%s\' to launch a local cluster, \'%s\' to autodetect ' +
            'the currently running EMR cluster, or specify an EMR cluster ID directly') % (AutoDaskCluster.ClusterType.LOCAL, AutoDaskCluster.ClusterType.EMR))

    def __init__(self, cluster_type=ClusterType.LOCAL):
        if cluster_type == None or cluster_type.upper() == self.ClusterType.LOCAL:
            # Don't spill over to disk... of course, spilling over to disk
            # allows processing of larger datasets, but at the cost of much
            # slower execution time (and may cause contention with other workers
            # trying to read data from the same disk filesystem). We might revisit
            # these settings after some testing, but I think it is worthwhile
            # for the local scheduler.
            dask.config.set({'distributed.worker.memory.target': False})
            dask.config.set({'distributed.worker.memory.spill': False})
            dask.config.set({'distributed.worker.memory.pause': 0.80})
            dask.config.set({'distributed.worker.memory.terminate': 0.98})

            self.__cluster_type = self.ClusterType.LOCAL
            self.__cluster = dask.distributed.LocalCluster()
            self.scheduler_address = self.__cluster.scheduler_address
        elif cluster_type.upper() == self.ClusterType.EMR:
            self.__cluster_type = self.ClusterType.EMR
            hostname = self.__find_running_emr_cluster_hostname()
            self.__init_emr_cluster_client(hostname)
        else:
            self.__cluster_type = self.ClusterType.EMR
            hostname = self.__find_running_emr_cluster_hostname(cluster_type)
            self.__init_emr_cluster_client(hostname)

    def __find_running_emr_cluster_hostname(self, cluster_id=None):
        emr_client = boto3.client('emr')

        if cluster_id is None:
            active_states = ['RUNNING', 'WAITING']
            response = emr_client.list_clusters(ClusterStates=active_states)
            clusters = response['Clusters']

            if len(clusters) == 0:
                raise RuntimeError('No EMR clusters in states %s' % active_states)
            elif len(clusters) > 1:
                raise RuntimeError('Multiple EMR clusters found: %s. Specify one using \'--cluster <cluster-id>\'.')
            else:
                cluster_id = clusters[0]['Id']

        response = emr_client.describe_cluster(ClusterId=cluster_id)
        return response['Cluster']['MasterPublicDnsName']

    def __find_dask_ports(self, timeout_sec=60):
        def extract_port(regex, output):
            match = regex.search(output)
            if match:
                return int(match.group(1))

        scheduler_regex = re.compile(r'Scheduler at:\s*tcp://[0-9\.]+:(\d+)')
        dashboard_regex = re.compile(r'dashboard at:\s*:(\d+)')
        scheduler_port = None
        dashboard_port = None

        start_time = time.time()

        while scheduler_port is None or dashboard_port is None:
            if time.time() - start_time > timeout_sec:
                raise RuntimeError('Could not get Dash cluster connection details after %d seconds' % timeout_sec)

            time.sleep(1)

            result = self.__connection.run('cat /home/hadoop/dask-yarn-cluster.log', hide=True)

            if scheduler_port is None:
                scheduler_port = extract_port(scheduler_regex, result.stdout)
                if scheduler_port:
                    print('Found scheduler port: %d' % scheduler_port)
            
            if dashboard_port is None:
                dashboard_port = extract_port(dashboard_regex, result.stdout)
                if dashboard_port:
                    print('Found dashboard port: %d' % dashboard_port)
    
        return (scheduler_port, dashboard_port)


    def __init_emr_cluster_client(self, emr_hostname):
        private_key = '/Users/danford/Dropbox/Skunkworks/Misc/Allen Institute Key pair.pem' # FIXME

        self.__connection = fabric.Connection(
            host = emr_hostname,
            user = 'hadoop',
            connect_kwargs = {
                'key_filename': private_key
            })

        self.__connection.put(os.path.join(os.path.dirname(__file__), 'etc/dask-yarn-cluster.py'), '/tmp/')
        self.__connection.run('/tmp/dask-yarn-cluster.py', hide=True)

        scheduler_port, dashboard_port = self.__find_dask_ports()

        self.__scheduler_forward = self.__connection.forward_local(scheduler_port)
        self.__scheduler_forward.__enter__()

        self.__dashboard_forward = self.__connection.forward_local(dashboard_port)
        self.__dashboard_forward.__enter__()

        self.scheduler_address = 'tcp://localhost:%d' % scheduler_port
        self.dashboard_address = 'http://localhost:%d' % dashboard_port

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.__cluster_type == self.ClusterType.LOCAL:
            self.__cluster.close()
        else:
            self.__connection.run('/tmp/dask-yarn-cluster.py stop', hide=True)
            self.__scheduler_forward.__exit__(*args)
            self.__dashboard_forward.__exit__(*args)
            self.__connection.close()

        return self

