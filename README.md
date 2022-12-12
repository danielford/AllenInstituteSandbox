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


