#!/bin/sh

# run unit tests inside qserv-ingest container
pip install pytest
pytest /ingest/python/ --log-cli-level=DEBUG --capture=tee-sys
