"""Module to assist in creating a Dask local cluster to be used in scripts

Exports a single class: AutoDaskCluster
"""

import os
import time
import re
import socket
from contextlib import closing
import boto3
from fabric import Connection
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


    def __init_emr_cluster_client(self, emr_hostname):
        private_key = '/Users/danford/Dropbox/Skunkworks/Misc/Allen Institute Key pair.pem'

        self.__connection = Connection(
            host = emr_hostname,
            user = 'hadoop',
            connect_kwargs = {
                'key_filename': private_key
            })

        self.__connection.put(os.path.join(os.path.dirname(__file__), 'etc/dask-yarn-cluster.py'), '/tmp/')
        result = self.__connection.run('/tmp/dask-yarn-cluster.py')

        regex = re.compile(r'YarnCluster scheduler port: (\d+)')

        match = regex.match(result.stdout)
        if match:
            scheduler_port = int(match.group(1))
        else:
            raise RuntimeError('Could not get YarnCluster scheduler port from result: %s' % result)

        time.sleep(10)

        self.__forward_context = self.__connection.forward_local(scheduler_port)
        self.__forward_context.__enter__()

        self.scheduler_address = 'tcp://localhost:%d' % scheduler_port

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.__cluster_type == self.ClusterType.LOCAL:
            self.__cluster.close()
        else:
            self.__connection.run('/tmp/dask-yarn-cluster.py stop', hide=True)
            self.__forward_context.__exit__(*args)
            self.__connection.close()

        return self

