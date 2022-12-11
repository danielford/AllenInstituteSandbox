#!/usr/bin/env python
"""Script to calculate cluster statistics using Dask's built-in groupby() method

"""


import os
import logging
import xarray
import shutil
import dask
import dask.distributed

from dask_cluster import init_dask_client

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

DATA_DIR = os.environ.get('DATA_DIR', '/data/AllenInstitute/')
INPUT_FILE = os.path.join(DATA_DIR, os.environ.get('INPUT_FILE', 'WB.postQC.rawcount.20220727.zarr'))
BASE_NAME = ".".join(os.path.basename(INPUT_FILE).split('.')[0:-1])
TASK_GRAPH_FILE = 'cluster-stats.svg'
OUTPUT_FILE = os.path.join(DATA_DIR, BASE_NAME + '.cl-stats.zarr')
XARRAY_LOAD = True  # load into memory?
DASK_VISUALIZE = False  # save task graph? sometimes is super slow, pegs the CPU for minutes

# Dask requires wrapping in a __name__ == '__main__' check
# in order to use the distributed client locally
if __name__ == '__main__':
    client = init_dask_client()
    logging.info("Dashboard link: %s" % client.dashboard_link)

    logging.info("Loading %s" % INPUT_FILE)
    data = xarray.open_zarr(INPUT_FILE)

    if XARRAY_LOAD:
        logging.info("Loading data into cluster memory...")
        data = data.persist()
        dask.distributed.wait(data)
        logging.info("Finished loading data")

    logging.info("Calculating task graph")
    results = data.groupby('cluster').mean(dim='cell')

    if DASK_VISUALIZE:
        logging.info("Saving task graph: %s" % TASK_GRAPH_FILE)
        dask.visualize(results, filename=TASK_GRAPH_FILE)

    logging.info("Executing task graph and streaming output to %s" % OUTPUT_FILE)
    shutil.rmtree(OUTPUT_FILE, ignore_errors=True)
    results.to_zarr(OUTPUT_FILE)

    logging.info("Finished")
