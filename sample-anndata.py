#!/usr/bin/env python
"""Script used to take a random sample of the large 'whole brain' AnnData matrix.

This script loads the AnnData matrix into memory (e.g., does not use 'backed' mode) so be careful
you have enough physical memory for that. You provide a sampling percentage as a float,
e.g. "--sample 0.01" results in a 1% sample (a 10% sample of rows and 10% sample of columns).
The output is written to the same location as the input with a renamed suffix.
"""

import sys
import argparse
import math
import os
import logging
import anndata

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

ARG_PARSER = argparse.ArgumentParser(prog=sys.argv[0], description=__doc__)
ARG_PARSER.add_argument('-s', '--sample', type=float, required=True,
    help="a sample percentage of the overall dataset (e.g., 0.01 for 1%% sample)")
ARG_PARSER.add_argument('filename', help='AnnData *.h5ad file to load', nargs=1)

args = ARG_PARSER.parse_args()
input_file = args.filename[0]
directory = os.path.dirname(input_file)
base_name = ".".join(os.path.basename(input_file).split('.')[0:-1])
output_file = os.path.join(directory, base_name + '.sample-%.2f.h5ad' % args.sample)
axis_sample = math.sqrt(args.sample)

logging.info("Reading from %s" % input_file)

# load into memory, takes about ~30min on r5.8xlarge instance
adata = anndata.read_h5ad(input_file)

logging.info("Taking %f random sample of genes (columns)" % axis_sample)
sampled_genes = adata.var.sample(math.floor(len(adata.var) * axis_sample)).index.tolist()

logging.info("Taking %f random sample of cells (rows)" % axis_sample)
sampled_cells = adata.obs.sample(math.floor(len(adata.obs) * axis_sample)).index.tolist()

logging.info("Original matrix is (%s), sampled matrix will be (%d. %d)" %
    (adata.shape, len(sampled_cells), len(sampled_genes)))

# subset the matrix with the new arrays for rows/columns
sampled_matrix = adata[sampled_cells, sampled_genes]

logging.info("Writing to %s" % output_file)
sampled_matrix.write(output_file)

logging.info("Finished")
