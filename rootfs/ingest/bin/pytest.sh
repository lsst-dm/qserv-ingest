#!/bin/sh

# run unit tests inside qserv-ingest container
pip install pytest
pytest /ingest/python/ --capture=tee-sys -vv
