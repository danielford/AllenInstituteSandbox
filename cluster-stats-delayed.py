#!/usr/bin/env python

import os
import time
import logging
import xarray
import shutil
import pandas
import itertools

import dask
import dask.delayed
from dask.distributed import Client, LocalCluster, wait, progress

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

DATA_DIR = os.environ.get('DATA_DIR', '/data/AllenInstitute/')
INPUT_FILE = os.path.join(DATA_DIR, os.environ.get('INPUT_FILE', 'WB.postQC.rawcount.20220727.zarr'))
BASE_NAME = ".".join(os.path.basename(INPUT_FILE).split('.')[0:-1])
TASK_GRAPH_FILE = 'cluster-stats.svg'
OUTPUT_FILE = os.path.join(DATA_DIR, BASE_NAME + '.cl-stats.parquet')
STATS = os.environ.get('STATS', 'mean,present,sqr_means')


# returns pandas.DataFrame with columns: cluster, gene, sum,  
def chunk_statistics(chunk, offset, cells, cluster_ids, genes, stats):
    df = pandas.DataFrame(chunk).groupby(cluster_ids.values).sum()
    df.columns = genes
    df['cluster'] = df.index
    return df.melt(id_vars=['cluster'], var_name='gene', value_name='sum')

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
    cells = ds.cell[offset[0]:offset[0]+chunk.shape[0]]
    cluster_ids = ds.cluster[offset[0]:offset[0]+chunk.shape[0]]
    genes = ds.gene[offset[1]:offset[1]+chunk.shape[1]]
    return dask.delayed(chunk_statistics)(chunk, offset, cells, cluster_ids, genes, STATS)


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
    cluster = LocalCluster(local_directory="/data/dask-worker-space")
    client = Client(cluster)

    logging.info("Dashboard link: %s" % client.dashboard_link)

    logging.info("Loading %s" % INPUT_FILE)
    data = xarray.open_zarr(INPUT_FILE)

    da = data.X.data  # access underlying dask array directly
    delayed_results = []

    for inds in itertools.product(*map(range, da.blocks.shape)):
        delayed_results.append(delayed_chunk_statistics(data, inds))

    results = combine_delayed_results(delayed_results).compute()

    logging.info("Finished computing statistics, writing to %s" % OUTPUT_FILE)
    results.to_parquet(OUTPUT_FILE)
