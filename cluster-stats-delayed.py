#!/usr/bin/env python
"""Script to calculate cluster statistics using a dask.delayed aproach.

This script loads the input file into Dask (optionally persisting into memory),
then computes cluster statistics by processing the array chunk-wise. The basic
algorithm is essentially the same as get_cl_stats_parquet in big_util.R, except
here we use Dask's built-in ability to iterate through a distributed dataset
chunk-by-chunk using 'dask.Array.blocks' method. Then we use dask.delayed to
parallelize it, as well as parallelizing the combining of multiple results from
each chunk.

The output is currently a Pandas dataframe written as a Parquet file.
"""

import os
import sys
import argparse
import logging
import xarray
import pandas
import itertools
import dask
import dask.delayed
import dask.distributed
from dask_cluster_helpers import AutoDaskCluster

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

ARG_PARSER = argparse.ArgumentParser(prog=sys.argv[0], description=__doc__)
ARG_PARSER.add_argument('dataset', help='Zarr directory to load from', nargs=1)
ARG_PARSER.add_argument('-v', '--visualize', help='Generate a Dask task graph visualization',
    action='store_true')
ARG_PARSER.add_argument('-p', '--persist', 
    help='Eager-load the dataset into memory using dask.Array.persist()', action='store_true')
AutoDaskCluster.add_argparse_args(ARG_PARSER)

TASK_GRAPH_FILE = 'dask-task-graph.svg'

# returns pandas.DataFrame with columns: cluster, gene, sum,  
def chunk_statistics(chunk, cluster_ids, genes):
    df = pandas.DataFrame(chunk).groupby(cluster_ids.values).sum()
    df.columns = genes
    df['cluster'] = df.index
    df = df.melt(id_vars=['cluster'], var_name='gene', value_name='sum')
    df = df[df['sum'] >= 0]
    return df

# takes two pandas.DataFrame with columns: cluster, gene, sum
# aggregates along cluster/gene, adding sums together
def combine_chunk_statistics(df1, df2):
    return pandas.concat([df1, df2]).groupby(['cluster', 'gene']).sum().reset_index()


# takes xarray.Dataset, returns pandas.DataFrame wrapped in dask.delayed()
def delayed_chunk_statistics(ds, chunk_index):
    da = ds.X.data
    chunk = da.blocks[chunk_index]
    offset = [a*b for (a,b) in zip(chunk_index, da.chunksize)]
    # metadata objects to pass in to chunk_statistics (methods called via
    # dask.delayed(...) are not supposed to access any global state)
    cluster_ids = ds.cluster[offset[0]:offset[0]+chunk.shape[0]]
    genes = ds.gene[offset[1]:offset[1]+chunk.shape[1]]
    return dask.delayed(chunk_statistics)(chunk, cluster_ids, genes)


# takes a list of pandas.DataFrame wrapped in dask.delayed
# returns a single dask.delayed pandas.DataFrame with the full statistics
def combine_delayed_results(results):
    def pairs(xs):
        for i in range(0, len(xs), 2):
            yield xs[i:i+2]
    if len(results) == 0:
        return None  # shouldn't happen...
    elif len(results) == 1:
        return results[0]
    else:
        new_results = []
        for pair in pairs(results):
            if len(pair) == 2:
                new_results.append(dask.delayed(combine_chunk_statistics)(pair[0], pair[1]))
            elif len(pair) == 1:
                new_results.append(dask.delayed(pair[0]))
        return combine_delayed_results(new_results)


# Dask requires wrapping in a __name__ == '__main__' check
# in order to use the distributed client locally
if __name__ == '__main__':
    args = ARG_PARSER.parse_args()
    input_file = args.dataset[0].rstrip(os.sep)
    directory = os.path.dirname(input_file)
    base_name = ".".join(os.path.basename(input_file).split('.')[0:-1])
    output_file = os.path.join(directory, base_name + '.cl-stats.parquet')

    with AutoDaskCluster(args.cluster) as cluster, dask.distributed.Client(cluster) as client:
        logging.info("Dashboard link: %s" % client.dashboard_link)

        logging.info("Loading %s" % input_file)
        data = xarray.open_zarr(input_file)

        if args.persist:
            logging.info("Persisting data into cluster memory...")
            data = data.persist()
            dask.distributed.wait(data)
            logging.info("Finished persisting data")

        logging.info("Computing statistics")

        da = data.X.data  # access underlying dask array directly
        delayed_results = []

        for inds in itertools.product(*map(range, da.blocks.shape)):
            delayed_results.append(delayed_chunk_statistics(data, inds))

        results = combine_delayed_results(delayed_results)

        if args.visualize:
            logging.info("Saving task graph: %s" % TASK_GRAPH_FILE)
            dask.visualize(results, filename=TASK_GRAPH_FILE)

        results = results.compute()

        logging.info("Finished computing statistics, writing to %s" % output_file)
        results.to_parquet(output_file)
