#!/usr/bin/env python

"""Script to convert the "whole brain" AnnData matrix into Zarr format using Xarray

This converts the file incrementally so memory consumption is fixed, e.g. it does not
load the entire source matrix into memory at once.

It also is not parallelized with Dask yet. For now it is useful to be able to run
this (slowly) on the entire source matrix (200+ GB) without needing enough Dask
worker capacity to be able to load it into memory.

Approximate runtime:
    ~52min on r5.8xlarge instance with full 214GB WB.postQC.rawcount.20220727.h5ad file
    ~1min on r5.large with 1% sample dataset (1.5GB)
"""

import os
import anndata
import shutil
import logging
import xarray

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

DATA_DIR = os.environ.get('DATA_DIR', '/data/AllenInstitute/')
INPUT_FILE = os.path.join(DATA_DIR, os.environ.get('INPUT_FILE', 'WB.postQC.rawcount.20220727.h5ad'))
BASE_NAME = ".".join(os.path.basename(INPUT_FILE).split('.')[0:-1])
OUTPUT_FILE = os.path.join(DATA_DIR, BASE_NAME + '.zarr')
RECHUNKED_OUTPUT_FILE = OUTPUT_FILE + '.rechunked'

ROW_BATCH_SIZE = 10_000  # number of rows to process at a time
FINAL_ZARR_CHUNK_SIZES = { 'cell': 8000, 'gene': 4000 }  # roughly 128MiB with float32 data

logging.info("Reading from %s" % INPUT_FILE)
logging.info("Storing Dask array at %s" % OUTPUT_FILE)


# load big anndata object in 'backed' mode (lazy-loads from disk as needed)
adata = anndata.read_h5ad(INPUT_FILE, backed='r')

shutil.rmtree(OUTPUT_FILE, ignore_errors=True)

# write rows incrementally to output file
for row_start in range(0, adata.shape[0], ROW_BATCH_SIZE):
    row_end = min(adata.shape[0], row_start + ROW_BATCH_SIZE)
    # slice anndata object (results in numpy array)
    np = adata[row_start:row_end, :].X.toarray()
    ds = xarray.Dataset({ 'X': xarray.DataArray(np, dims=('cell', 'gene')) })
    if row_start == 0:
        ds.to_zarr(OUTPUT_FILE)
    else:
        ds.to_zarr(OUTPUT_FILE, append_dim='cell')
    logging.info("Processed %d / %d rows" % (row_end, adata.shape[0]))

logging.info("Finished writing to %s" % OUTPUT_FILE)


logging.info("Rechunking and adding metadata...")

cells = list(adata.obs_names)
genes = list(adata.var_names)
cluster_assns = adata.obs.cl.astype('int')

ds = xarray.open_zarr(OUTPUT_FILE).unify_chunks()

ds = ds.assign_coords({
    'cell': cells,
    'gene': genes,
    'cluster': ('cell', cluster_assns),
})

# workaround for bug where xarray does not reset zarr
# chunk metadata after rechunking
# https://github.com/pydata/xarray/issues/2300
ds.encoding.pop('chunks', None)
ds.X.encoding.pop('chunks', None)
ds.cluster.encoding.pop('chunks', None)

ds = ds.chunk(FINAL_ZARR_CHUNK_SIZES)

shutil.rmtree(RECHUNKED_OUTPUT_FILE, ignore_errors=True)
ds.to_zarr(RECHUNKED_OUTPUT_FILE)

shutil.rmtree(OUTPUT_FILE)
shutil.move(RECHUNKED_OUTPUT_FILE, OUTPUT_FILE)
