#!/usr/bin/env python

import os
import xarray
import code
from dask.distributed import Client, LocalCluster, wait, progress


DATA_DIR = os.environ.get('DATA_DIR', '/data/AllenInstitute/')
INPUT_FILE = os.path.join(DATA_DIR, os.environ.get('INPUT_FILE', 'WB.postQC.rawcount.20220727.zarr'))

# Dask requires wrapping in a __name__ == '__main__' check
# in order to use the distributed client locally
if __name__ == '__main__':
    cluster = LocalCluster(local_directory="/data/dask-worker-space")
    client = Client(cluster)

    print("Dask dashboard link: %s" % client.dashboard_link)

    data = xarray.open_zarr(INPUT_FILE)

    print("Dataset at %s available in 'data' variable" % INPUT_FILE)

    # launch an interactive python session with 'data'
    #  available as an xarray.Dataset
    code.InteractiveConsole(locals=globals()).interact()
