#!/usr/bin/env python
"""Script to calculate cluster statistics using Dask's built-in groupby() method.

This script loads the input file into Dask (optionally persisting into memory),
then computes cluster statistics by using built-in Dask groupby and aggregations.

Unfortunately, so far, I have found this method to be very slow. With enough
memory, it does eventually complete, but it results in very large task graphs
and a large amount of intermediate objects in memory.
"""

import os
import sys
import argparse
import logging
import xarray
import shutil
import dask
import dask.distributed
from dask_cluster import init_dask_client

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

ARG_PARSER = argparse.ArgumentParser(prog=sys.argv[0], description=__doc__)
ARG_PARSER.add_argument('dataset', help='Zarr directory to load from', nargs=1)
ARG_PARSER.add_argument('-v', '--visualize', help='Generate a Dask task graph visualization',
    action='store_true')
ARG_PARSER.add_argument('-p', '--persist', 
    help='Eager-load the dataset into memory using dask.Array.persist()', action='store_true')

TASK_GRAPH_FILE = 'dask-task-graph.svg'

# Dask requires wrapping in a __name__ == '__main__' check
# in order to use the distributed client locally
if __name__ == '__main__':
    args = ARG_PARSER.parse_args()
    input_file = args.dataset[0].rstrip(os.sep)
    directory = os.path.dirname(input_file)
    base_name = ".".join(os.path.basename(input_file).split('.')[0:-1])
    output_file = os.path.join(directory, base_name + 'cl-stats.zarr')

    client = init_dask_client()
    logging.info("Dashboard link: %s" % client.dashboard_link)

    logging.info("Loading %s" % input_file)
    data = xarray.open_zarr(input_file)

    if args.persist:
        logging.info("Loading data into cluster memory...")
        data = data.persist()
        dask.distributed.wait(data)
        logging.info("Finished loading data")

    logging.info("Calculating task graph")
    results = data.groupby('cluster').mean(dim='cell')

    if args.visualize:
        logging.info("Saving task graph: %s" % TASK_GRAPH_FILE)
        dask.visualize(results, filename=TASK_GRAPH_FILE)

    logging.info("Executing task graph and streaming output to %s" % output_file)
    shutil.rmtree(output_file, ignore_errors=True)
    results.to_zarr(output_file)

    logging.info("Finished")
