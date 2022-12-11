"""Module to assist in creating a Dask local cluster to be used in scripts

Exports a single function: init_dask_cluster()

The function creates a local Dask cluster using the distributed scheduler
(see: https://distributed.dask.org/en/stable/index.html). This mode of
using Dask only uses the resources (CPU, memory) of your local machine,
however it does parallelize across those, AND scripts written using this
can be easily ran on a much larger multi-node cluster simply by changing
to using a remote cluster.
"""

import dask
import dask.distributed

def init_dask_client() -> dask.distributed.Client:

    # Got errors doing a big groupby() without this... haven't tested fully either way
    dask.config.set({'array.slicing.split_large_chunks': False})

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

    cluster = dask.distributed.LocalCluster()
    return dask.distributed.Client(cluster)

