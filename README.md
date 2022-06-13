# qserv-ingest

Tools for parallel data ingest inside Qserv using Qserv replication service

[![Build Status](https://travis-ci.com/lsst-dm/qserv-ingest.svg?branch=master)](https://travis-ci.com/lsst-dm/qserv-ingest)

[Documentation](https://qserv-ingest.lsst.io)

## Documentation for the new Ingest system

[Home page](https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850)

### Regular tables

[Notes on ingesting regular fully-replicated tables](https://confluence.lsstcorp.org/display/DM/Ingest%3A+7.+Notes+on+ingesting+regular+%28fully-replicated%29+tables)

https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion8)-Locateregulartables
https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion8)-Ingestingasinglefile

## How to publish a new release

```
RELEASE="2021.8.1-rc1"
./publish-release.sh $RELEASE
```
