#!/usr/bin/env python

import os
import logging
import xarray
import shutil

from dask.distributed import Client, LocalCluster, wait, progress

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

DATA_DIR = os.environ.get('DATA_DIR', '/data/AllenInstitute/')
INPUT_FILE = os.path.join(DATA_DIR, os.environ.get('INPUT_FILE', 'WB.postQC.rawcount.20220727.zarr'))
BASE_NAME = ".".join(os.path.basename(INPUT_FILE).split('.')[0:-1])
TASK_GRAPH_FILE = 'cluster-stats.svg'
OUTPUT_FILE = os.path.join(DATA_DIR, BASE_NAME + '.cl-stats.zarr')

# Dask requires wrapping in a __name__ == '__main__' check
# in order to use the distributed client locally
if __name__ == '__main__':
    cluster = LocalCluster(local_directory="/data/dask-worker-space")
    client = Client(cluster)

    logging.info("Dashboard link: %s" % client.dashboard_link)

    logging.info("Loading %s" % INPUT_FILE)
    data = xarray.open_zarr(INPUT_FILE)

    logging.info("Computing statistics")
    results = data.groupby('cluster').mean(dim='cell')

    # leaving this out for now, it is extremely slow for
    # some reason on this computation
    # 
    # logging.info("Saving task graph: %s" % TASK_GRAPH_FILE)
    # dask.visualize(results, filename=TASK_GRAPH_FILE)

    logging.info("Writing output to %s" % OUTPUT_FILE)
    shutil.rmtree(OUTPUT_FILE, ignore_errors=True)
    results.to_zarr(OUTPUT_FILE)

    logging.info("Finished")
