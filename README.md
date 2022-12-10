# AllenInstituteSandbox
Personal sandbox for Python development work for Allen Institute

## Package setup

Create a Python virtual environment (venv) and install dependencies into it:
```
# run these commands from the package root
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```


## Adding new dependencies

```
# make sure you are in the virtual environment before running these:
python -m pip install <package>
python -m pip freeze > requirements.txt
```
