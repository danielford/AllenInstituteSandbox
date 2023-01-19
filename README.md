# Allen Institute Sandbox
Sandbox repository for Python development work for Allen Institute

## Package setup
*Note*: I've changed this package to use [Conda](https://conda.io) for dependency management instead of [pip](https://pypi.org/project/pip/) because that made it convenient to integrate with [dask-yarn](https://yarn.dask.org/en/latest/aws-emr.html) which is designed to work with a Conda environment. If you set this package up before, just `rm -rf .venv/` before following the instructions below.

First, install Conda to your local machine using the minimal installer [Miniconda](https://docs.conda.io/en/latest/miniconda.html). I've tested this package on both Linux and MacOS (Windows should be possible in theory, but I have not tested it).

Next, check the package out from GitHub:
```shell
git clone git@github.com:danielford/AllenInstituteSandbox.git
cd AllenInstituteSandbox
```

Once the package is checked out, create a Conda environment from the template `environment.yml`. 

```shell
# creates a new Conda environment under .env/ and installs the packages listed in environment.yml
conda env create --prefix .env -v -f ./environment.yml

# you can always easily 'rm -rf .env' and recreate it again later if needed
```
*Warning:* Conda environment creation is really slow. At the time of this writing, it takes about 40-50 minutes to complete a fresh environment creation. Maybe it can be improved at some point... 😩

Next, you need to activate the environment in your shell:

```shell
conda activate $(pwd)/.env
```

From there, you should be able to run any script like so:
```shell
./convert-to-zarr.py --arg1 --arg2 ...etc
```

Also, just invoking `python` will find the correct version from your Conda environment.

### Missing Dependency Errors

If you get errors about missing dependencies (e.g. during `import` statements), you are probably running in a new shell without the Conda environment activated. Activate the environment again to fix it:
```shell
conda activate $(pwd)/.env
```

Another possibility is that you've pulled down a new commit that added a new dependency. In that case, you just need to update from the `environment.yml` file again:
```shell
conda env update --prune --file environment.yml
```

## Adding new dependencies
New dependencies should be added to `environment.yml` under `dependencies`. These can be just naked package names, or version specifications such as `python=3.10`, e.g. exactly what you would pass to `conda install`.

You can, of course, install packages directly using `conda install`. However, unless you add them to `environment.yml`, others who check out the package won't automatically get those dependencies installed.

After adding or removing dependencies from `environment.yml`, simply update the environment using Conda:
```shell
conda env update --prune --file environment.yml
```

Don't forget to commit and push the new `environment.yml`!

## Running Scripts
As long as your shell has the Conda environment activated (you should see `(/path/to/AllenInstituteSandbox/.env)` in your shell prompt), you should be able to easily run the scripts from the commandline:
```shell
./convert-to-zarr.py /path/to/input.h5ad
```

You can also just run `python` to drop into an interactive shell, or use the `open-dataset.py` script to do the same after opening a dataset.


## Development Environment
You can use any development environment or text editor you like, as long as you can check out this repo and you have Python3 and can install packages via Conda. However, here are some details about how my environment is setup in case it is helpful:

As an editor/IDE for Python, I really like [Visual Studio Code](https://code.visualstudio.com/). It's free, open-source, very customizable, and has lots of plugins/themes available in the community.

I installed the following VS Code extensions (you do so from the Extensions menu on the left):
* Python
* Pylance
* R
* Remote - SSH
* Remote Explorer

There are tons of others available of course, including one to add support for Jupyter Notebooks. I assume it lets you run your notebook from directly inside of VS Code, which could be cool. I haven't explored that, but if I get Jupyter working I'll add those details here.

### Remote Development
The Remote extensions are particularly important, since that's often how I like to develop. I will check out this Git repository somewhere on my Linux server. Then I use the Remote - SSH extension to SSH into my server and open the folder where I've checked out the repo. VS Code then acts as if all the files are local, you can edit files all you want, even run or debug scripts, and its all happening on the remote server. However, VS Code is making it appear that all the files are local to your laptop while it simply synchronizes changes to your server in the background. This means you get a fast, snappy editing experience locally (compared to editing files directly over an SSH connection, which sometimes has a lot of latency), but when you actually run your scripts you have the full power of your big beefy Linux server.

I usually also have the repository checked out locally as well, and I have two different VS Code workspaces for each one (in the File menu you have options for adding additional folders to your current workspace, or saving the current workspace to a file that you can load from again).

Once you've installed the above extensions, you can go to the Remote Explorer tab and click on the little gear, which should bring up a command prompt where you can create or edit a custom SSH configuration file. This is where you define the server that VS Code will SSH into. Here's what mine looks like (this file lives on my Mac laptop):

`/Users/danford/Workspace/remote-servers.ssh`
```
Host AllenInstitute
  HostName ai-dev.aws.danford.dev
  IdentityFile "/Users/danford/Dropbox/Skunkworks/Misc/Allen Institute Key pair.pem"
  User ec2-user
```

The first line gives the server a friendly name, I chose "AllenInstitute" but it can be anything. The rest should be fairly self explanatory, you put in the hostname and username you use to SSH onto your Linux server. If you use password authentication instead of keypair, then you will not need the `IdentityFile` line.

If you have multiple remote servers that you work on, then you can add them all here. Any servers listed here will get shown when you open up the Remote Explorer tab in VS Code.

Once you save that and go back to your Remote Explorer tab, you should see the server available and you can connect and open a folder on the remote server. VS Code will probably prompt you to install some of the extensions on the remote host, make sure you do so otherwise the Python language features won't work.

If you already checked out this repository on your remote server, then you can just navigate to that folder. Otherwise you can just choose where you want your workspace to live (e.g., home directory, whatever) and then clone the repo from within VS Code (cmd+shift+P to bring up the Command Palette, then search for the 'Git: Clone' command). Make sure to run the commands in the Package Setup section to set up the Python virtual environment after cloning the package.

You can open the VS Code terminal (View -> Terminal) to run commands on the remote server. This is handy for doing this package setup, or for running scripts, running Python interactively, etc.


#### Port Forwarding
One reason I like VS Code's remote development plugin is that it automatically detects when your scripts are running local web servers (e.g. for the Dask Dashboard I was showing earlier) and it will automatically forward the ports correctly over the SSH tunnel for you. If you have the VS Code terminal open (View -> Terminal) and it sees a string like `http://localhost:8787` it will automatically forward local port 8787 over the SSH connection so you can load the web dashboard on your laptop. You will probably see a popup window that has a button you can press to open it directly, otherwise you can see the forwarded ports under the "Ports" tab in the bottom panel.


### Python Integration
If you've installed the Python extension (both locally as well as in the remote server), VS Code will do a bunch of nice type-checking of your Python code, you will see method suggestions in dropdowns, etc. In order for this to work, VS Code needs to be using the Python version that is in your virtual environment (see "Package Setup" section above). If you already have the virtual environment created, VS Code should auto-detect this, but if it does not (or if you created it after launching VS Code), then you can open the Command Palette (cmd+shift+P), type 'Python: Select Interpreter', and then point it at `.env/bin/python`. Then it should be able to find all the libraries you installed, etc.


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
* In addition to (or in parallel with) setting up the cluster, would be trying to implement a larger function or workflow step using Dask. A more real-world example to work against will also help in evaluating and optimizing the performance characteristics.
