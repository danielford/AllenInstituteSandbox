#!/usr/bin/env python
"""Script to open one or more datasets in an interactive Python environment

This script currently works on a few file types. The files must be
named using the appropriate file extension to be recognized:
    *.h5ad: AnnData, see https://anndata.readthedocs.io/en/latest/
    *.parquet: Parquet using pyarrow
    *.zarr: Zarr array created using Xarray (see
      https://docs.xarray.dev/en/stable/user-guide/io.html#zarr)

Once the filetype is recognized, the script will jump into an
interactive Python session. The dataset(s) will be available
in variables 'data1', 'data2', etc. The actual data may or may
not be in memory depending on the file type. It will for Parquet
and AnnData, but Zarr will lazy-load using Dask until you run
"data = data.persist()" to force the data into Dask memory space.

A Dask local cluster will be automatically instantiated and used
if necessary. The dashboard link will be printed to the console
(you may need to forward SSH ports in order to access it in your
browser). Dask workers are configured to only operate in memory,
so your operations may get stuck or perform slowly if you try to
do too many or too large computations.

Usage:
    $ ./open-dataset.py [PATH] ...
"""

import os
import sys
import code
import argparse
from dask_cluster import init_dask_client

ARG_PARSER = argparse.ArgumentParser(prog=sys.argv[0], description=__doc__)
ARG_PARSER.add_argument('path', help='Full path to a dataset to load', nargs='+')

def open_dataset(path):
    ext = path.split('.')[-1:][0].lower()

    if ext == 'zarr':
        import xarray
        return xarray.open_zarr(path)
    elif ext == 'parquet':
        import pandas
        return pandas.read_parquet(path)
    elif ext == 'h5ad':
        import anndata
        return anndata.read_h5ad(path)
    else:
        raise ValueError("Unrecognized dataset type: %s" % path)

# Dask requires wrapping in a __name__ == '__main__' check
# in order to use the distributed client locally
if __name__ == '__main__':
    args = ARG_PARSER.parse_args()

    client = init_dask_client()
    print("Dask dashboard link: %s" % client.dashboard_link)

    print("\n\nDatasets:")

    _g = globals()
    datasets = args.path
    var_names = ['data%d' % i for i in range(1, len(datasets)+1)]

    for path, var_name in zip(datasets, var_names):
        path = path.rstrip(os.sep)
        print("* %s: %s" % (var_name, path))
        _g[var_name] = open_dataset(path)

    print("\n")

    # launch an interactive python session
    code.InteractiveConsole(locals=_g).interact()
