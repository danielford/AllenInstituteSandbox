# Allen Institute Sandbox
Sandbox repository for Python development work for Allen Institute

## Package setup
*Note*: I’m running this on Python 3.7.10, but probably any modern Python3 will do.

Once the package is checked out, create a Python virtual environment (venv) and install dependencies into it. Run these commands from the package root:
```shell
# create the virtual environment at <package-root>/.venv/
python3 -m venv .venv

# set up shell helpers (so that running 'python' will use the correct version and dependencies)
source .venv/bin/activate

# install dependencies listed in requirements.txt into the virtual environment
python -m pip install -r requirements.txt

# you can always easily 'rm -rf .venv' and recreate it again if needed
```

From there, you should be able to run any script like so:
```shell
./convert-to-zarr.py --arg1 --arg2 ...etc
```

### Missing Dependency Errors

If you get errors about missing dependencies (e.g. during `import` statements), you are probably running in a new shell without the virtual environment activated. Activate the environment again to fix it:
```shell
source .venv/bin/activate
```

Another possibility is that you've pulled down a new commit that added a new dependency. In that case, you just need to install from the `requirements.txt` again:
```shell
python -m pip install -r requirements.txt
```

## Adding new dependencies
```shell
# make sure you are in the virtual environment before running these:
python -m pip install <package-name>
python -m pip freeze > requirements.txt

# don't forget to commit the new requirements.txt!
```

## Running Scripts
As long as your shell has the virtual environment activated (you should see `(.venv)` in your shell prompt), you should be able to easily run the scripts from the commandline:
```shell
./convert-to-zarr.py /path/to/input.h5ad
```

You can also just run `python` to drop into an interactive shell, or use the `open-dataset.py` script to do the same after opening a dataset.


# Progress Updates

## December 13, 2022
I thought it was a good time to give a formal update and go over what I’ve done and learned so far, as well as some ideas for next steps.

### Technology & Infrastructure Research
As we discussed before before, I looked into a bunch of different technologies for approaching this problem (large sparse 2-d matrix operations). Just to recap, our high-level conclusions were:
* GPU computing looks interesting, but maybe is too specialized/niche — harder to program, harder to understand performance characteristics, etc.
* Spark is a common recommendation for “big data” problems, however it is primarily meant for data frames (fixed, relatively small number of columns) and does not chunk datasets “vertically” (e.g. by columns). I ran into all kinds of weird errors trying to process your datasets with 30k+ columns.
* Dask seemed to be the best choice, since it natively supports chunking n-dimensional arrays across a cluster of machines, integrates well with other things like Pandas, Xarray, Zarr, etc., and is in Python.

After some more experimentation with Dask, Xarray, and Zarr, I found out a few more things:
* It’s really important that the data on disk (or in a cloud bucket) is stored in a format that is natively chunked (separate files) so that it can be read in parallel and disk IO does not become a bottleneck. It’s also important that the chunks should be aligned with how you want the dataset chunked in memory, so that you’re not re-chunking things on-the-fly (which causes a lot of wasted IO cycles, or huge intermediate objects stored in memory). More on this topic in this Dask blog post: [Choosing good chunk sizes in Dask](https://blog.dask.org/2021/11/02/choosing-dask-chunk-sizes)
* Zarr looks to be a good format since it is compressed and chunked. However, there’s a lot of different ways to store Zarr data, the file format itself is pretty low-level. Dask seems to [support it directly](https://docs.dask.org/en/stable/generated/dask.array.from_zarr.html), however I could not figure out how to create a Zarr file on disk from outside of Dask that would load in Dask.
* [Xarray also supports Zarr](https://docs.xarray.dev/en/stable/user-guide/io.html#io-zarr) (and runs on top of Dask), and it seems to be a bit more full-featured than Dask’s support:
	* It supports [reading and writing to/from cloud storage buckets](https://docs.xarray.dev/en/stable/user-guide/io.html#cloud-storage-buckets)
	* It uses [consolidated metadata](https://docs.xarray.dev/en/stable/user-guide/io.html#consolidated-metadata), which reduces IO operations
	* It supports [appending to an existing Zarr store](https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores), which was useful in converting from AnnData to Zarr without requiring holding the full dataset in memory (see below)
* Xarray is also more full-featured than plain old Dask arrays, e.g. similar to AnnData you can store multiple arrays with a shared set of coordinates/axis labels, additional variables, etc., and it all gets written together into Zarr and is available on the same `xarray.Dataset` object.
* One big drawback that I have not yet solved is that Xarray does not support sparse data in-memory (at least, not yet — I saw some GitHub issues and it looks like it may be in progress). The Zarr file format seems to perform really well on sparse array data (the same AnnData object converted into Zarr is several times smaller on disk), however in-memory I think it is still stored “densely”. I’m seeing very large memory consumption in my Dask scripts, and I think this may be (one) culprit.
* Currently, I am still working with Dask on a single machine (not using a cluster yet), however I am using Dask’s distributed scheduler locally, as described here: [Deploy Dask Clusters — Dask  documentation](https://docs.dask.org/en/stable/deploying.html#deploy-dask-clusters). This means the API and the performance characteristics are the same, but all the workers are running on the local machine. Once we need to, I can set up a remote Dask cluster and change `dask_cluster.py` to point to a remote cluster and all our scripts will automatically run against the cluster instead.

### Utility Scripts
I wrote a few Python scripts to assist in a few operations.
* `sample-anndata.py` takes an AnnData matrix stored as `.h5ad`and takes a random sample, resulting in a smaller AnnData file that is a random subset (e.g. 10% random sample of rows and 10% random sample of columns, resulting in a matrix 1% of the size of the original). I’m using this a lot to play around with Dask and Xarray without needing a lot of compute power.
* `open-dataset.py` takes one or more datasets on disk and loads them into an interactive Python interpreter where I can play around with them. Supports Parquet (using Pandas), Zarr (using Xarray), and AnnData. It also starts up a local Dask distributed cluster, which you can monitor through the dashboard.
* `convert-to-zarr.py` takes a potentially very large AnnData file,  and converts it iteratively to the Xarray Zarr format. Although this uses Xarray, it does not use Dask so you can run it on a machine without a lot of memory (it doesn’t need to load the AnnData file into memory). However, it properly chunks data so that it can be later loaded in a Dask cluster in parallel.

### Example Computation Scripts
There are two basic approaches to implementing your `get_cl_stats_parquet` algorithm from `big_util.R`. You can use Xarray’s native support for `groupby` aggregations, or you can do something conceptually similar to `get_cl_stats_parquet` and process each chunk of the array separately, combining results at the end. I tried both of these approaches:
* `cluster-stats-groupby.py` tries to use Xarray’s native `groupby` functionality on the entire array. It works, however it is extremely slow and extremely memory intensive. I had to run it on an EC2 `r5.24xlarge` instance (96 cores, 768GB memory), and it still took over 30 minutes to complete. With less memory available, it just stalled or required spilling lots of data to disk which slowed the computations to a crawl.
* `cluster-stats-delayed.py` tries to do the same basic approach as in `get_cl_stats_parquet`, however instead of having to transform the data into chunked Parquet files (as you did), we can take advantage of Dask’s chunking. Dask provides [dask.array.Array.blocks](https://docs.dask.org/en/stable/generated/dask.array.Array.blocks.html) which lets you iterate through the underlying chunks of the Dask array. Then I also used [Dask Delayed](https://docs.dask.org/en/stable/delayed.html), which lets you set up arbitrary task graphs of computations to be executed in parallel across a Dask cluster. I used this to process all the chunks in parallel, and from there the basic algorithm is pretty similar to the R code.

Right now, both versions are way too slow… both take 30+ minutes on the full dataset and require gobs of memory. I haven’t done a proper apples-to-apples comparison (e.g. subsetting the AnnData matrix to feed in exactly the same set of cells/clusters as the R code is doing), but I recall the R code was processing around 1MM cells / 50 clusters, in about 90 seconds. I have some ideas for how to get there:
* Probably the large number of clusters is making this way more intensive than it needs to be. The raw dataset has 100x the number of clusters as in the example. Maybe once I do a proper comparison to the R code, it won’t be quite as far off
* If I can figure out how to store sparse data in memory instead of dense, that may reduce the memory consumption significantly

Until we optimize further, I can't say which approach is really better. The groupby approach I felt was more similar to many other operations you'll presumably do on Xarray matrices. Groupby operations may be somewhat of an exception, but mostly these native operations will be much faster than trying to do the parallelism/chunking explicitly.

Although maybe it won't turn out to be optimal here, I wanted to explore Dask.Delayed because that's how we'll presumably parallelize things like the K-nearest-neighbors algorithm, where we have a 3rd party library that doesn't run on Dask.


### Notes on Dask

Right now, any of the scripts above that use Dask are set up to use the local distributed cluster. This means Dask automatically figures out how much CPU/memory you have on your local machine and it spawns enough workers to utilize it, but all the processes are still running on a single machine.

For now, I've been trying to focus on learning how to use Dask as a user/developer and not worrying too much about setting up complex infrastructure or optimizing too deeply. In the future, once I've deployed a remote Dask cluster, I can point to that in `dask_cluster.py` and scripts will automatically begin using that. It won't change how you interact with Dask objects or how you monitor the performance, in theory it will just give us access to more compute power.

Even though we're running locally, we can still use all the Dask tools for debugging and optimizing. For example, all the scripts will print out a link to a web dashboard where you'll see a bunch of diagnostics about the running cluster. See [Dashboard Diagnostics - Dask](https://docs.dask.org/en/stable/dashboard.html). You may need to set up [port forwarding](https://docs.dask.org/en/stable/diagnostics-distributed.html#port-forwarding) if you're running Dask on a remote machine over SSH, like I do.


### Next Steps

These are not necessarily in priority order, I'm just brainstorming some different areas that I think might be productive:

* I still want to setup a proper comparison of the cluster stats scripts against the R version. I suspect the large number of clusters is significantly impacting the performance. I don't know what case is more realistic (or whether small clusters are trimmed explicitly for this reason in your existing code). But it would help us to better understand exactly how Dask is comparing to your existing implementation.
* I want to investigate whether we can get Xarray to use a sparse matrix in-memory, as I also think it may significantly impact the performance (right now the memory requirements for loading the entire 4mm x 32k matrix into Dask cluster memory is enourmous). From [some Google searching](https://www.google.com/search?q=xarray+sparse) it looks plausible, there's even a [closed GitHub issue](https://github.com/pydata/xarray/issues/1375), but it needs some research.
* Once we are happy with the performance of some Dask algorithms running locally (parallelized, but on a single machine), the next step will be to set up a remote cluster, set up some form of auto-scaling and idle-shutdown behavior, then get these scripts connected to it and have them reading to / writing from S3.

