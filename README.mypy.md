# Run mypy for qserv-ingest

## Pre-requisites

Python3.10 is required (this could be set using virtualenv)

```bash
python3 -m pip install mypy  types-PyYAML sqlalchemy-stubs
```

## Run mypy

```bash 
./code-checks.sh -m
```
