#!/usr/bin/env python

"""
Script used to take a 1% random sample of the "whole brain" matrix

Usage:
    $ DATA_DIR=/path/to/dir INPUT_FILE=WB.postQC.rawcount.20220727.h5ad ./sample-anndata.py
"""

import anndata
import math
import os
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

# percentage of each dimension to sample
# 
# use 0.10 for overall 1% sample (.10 * .10 == 1%)
# use 0.32 for overall 10% sample (.32 * .32 =~ 10%)
# use 0.225 for overall 5% sample (.225 * .225 =~ 5%)
SAMPLE_PCT = float(os.environ.get('SAMPLE_PCT', '0.10'))

DATA_DIR = os.environ.get('DATA_DIR', '/data/AllenInstitute/')
INPUT_FILE = os.path.join(DATA_DIR, os.environ.get('INPUT_FILE', 'WB.postQC.rawcount.20220727.h5ad'))
BASE_NAME = ".".join(os.path.basename(INPUT_FILE).split('.')[0:-1])
OUTPUT_FILE = os.path.join(DATA_DIR, BASE_NAME + ('.sample-%.2f.h5ad' % math.pow(SAMPLE_PCT, 2)))

logging.info("Reading from %s" % INPUT_FILE)

# load into memory, takes about ~30min on r5.8xlarge instance
adata = anndata.read_h5ad(INPUT_FILE)

logging.info("Taking %f random sample of genes (columns) and cells (rows)" % SAMPLE_PCT)

# take sample of genes (columns)
sampled_genes = adata.var.sample(math.floor(len(adata.var) * SAMPLE_PCT)).index.tolist()

# take sample of cells (rows)
sampled_cells = adata.obs.sample(math.floor(len(adata.obs) * SAMPLE_PCT)).index.tolist()

logging.info("Original matrix is (%s), sampled matrix will be (%d. %d)" %
    (adata.shape, len(sampled_cells), len(sampled_genes)))

# result is (SAMPLE_PCT^2) sample of original matrix
sampled_matrix = adata[sampled_cells, sampled_genes]

logging.info("Writing to %s" % OUTPUT_FILE)
sampled_matrix.write(OUTPUT_FILE)

logging.info("Finished")
