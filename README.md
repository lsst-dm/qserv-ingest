# qserv-ingest

Tools for parallel data ingest inside Qserv using Qserv replication service

[![Build Status](https://travis-ci.com/lsst-dm/qserv-ingest.svg?branch=master)](https://travis-ci.com/lsst-dm/qserv-ingest)

[Documentation](https://qserv-ingest.lsst.io)

## Documentation for the new Ingest system
https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850

## How to publish a new release

```
RELEASE="2021.06.01-rc1"
git tag -a "$RELEASE" -m "Version $RELEASE"
git push --tag
./build-image.sh
./push-image.sh
```
