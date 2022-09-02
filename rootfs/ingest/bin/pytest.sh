#!/bin/sh

# WARN some tests use the same mock database and cannot be run concurrently
# i.e. do not use pytest '-n' option 

# run unit tests inside qserv-ingest container
pip install pytest==7.1.2
pytest /ingest/python/ --capture=tee-sys -vv -m "not scale"

# TODO, add option for
# pytest /ingest/python/ --capture=tee-sys -vv -m "scale"
