#!/usr/bin/env python
"""Script to convert the "whole brain" AnnData matrix into Zarr format using Xarray

This converts the file incrementally so memory consumption is fixed, e.g. it does not
load the entire source matrix into memory at once.

It also is not parallelized with Dask. For now it is useful to be able to run
this (slowly) on the entire source matrix (200+ GB) without needing enough Dask
worker capacity to be able to load it into memory.
"""

import os
import sys
import argparse
import anndata
import shutil
import logging
import xarray

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

ROW_BATCH_SIZE = 10_000  # number of rows to process at a time
FINAL_ZARR_CHUNK_SIZES = { 'cell': 8000, 'gene': 4000 }  # roughly 128MiB with float32 data

ARG_PARSER = argparse.ArgumentParser(prog=sys.argv[0], description=__doc__)
ARG_PARSER.add_argument('filename', help='AnnData *.h5ad file to convert', nargs=1)

args = ARG_PARSER.parse_args()
input_file = args.filename[0]
directory = os.path.dirname(input_file)
base_name = ".".join(os.path.basename(input_file).split('.')[0:-1])
output_file = os.path.join(directory, base_name + '.zarr')
rechunked_output_file = output_file + '.rechunked'

logging.info("Reading from %s" % input_file)
logging.info("Storing Dask array at %s" % output_file)

# load big anndata object in 'backed' mode (lazy-loads from disk as needed)
adata = anndata.read_h5ad(input_file, backed='r')

shutil.rmtree(output_file, ignore_errors=True)

# write rows incrementally to output file
for row_start in range(0, adata.shape[0], ROW_BATCH_SIZE):
    row_end = min(adata.shape[0], row_start + ROW_BATCH_SIZE)
    # slice anndata object (results in numpy array)
    np = adata[row_start:row_end, :].X.toarray()
    ds = xarray.Dataset({ 'X': xarray.DataArray(np, dims=('cell', 'gene')) })
    if row_start == 0:
        ds.to_zarr(output_file)
    else:
        ds.to_zarr(output_file, append_dim='cell')
    logging.info("Processed %d / %d rows" % (row_end, adata.shape[0]))

logging.info("Finished writing to %s" % output_file)


logging.info("Rechunking and adding metadata...")

cells = list(adata.obs_names)
genes = list(adata.var_names)
cluster_assns = adata.obs.cl.astype('int')

ds = xarray.open_zarr(output_file).unify_chunks()

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

shutil.rmtree(rechunked_output_file, ignore_errors=True)
ds.to_zarr(rechunked_output_file)

shutil.rmtree(output_file)
shutil.move(rechunked_output_file, output_file)
