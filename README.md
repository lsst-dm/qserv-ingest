# qserv-ingest

Tools for parallel data ingest inside Qserv using Qserv replication service

[![Build Status](https://travis-ci.com/lsst-dm/qserv-ingest.svg?branch=master)](https://travis-ci.com/lsst-dm/qserv-ingest)

## Documentation for the new Ingest system
https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850

## Examples

Based on [HSC live demo](https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog)
and https://confluence.lsstcorp.org/display/DM/Steps+of+ingesting+a+small+DESC-DC2+dataset+to+Qserv

Example workflow:
https://github.com/hsinfang/qserv-ingest-hsc-poc

## Input data

csv files: /sps/lsst/users/elles/qserv_install_local/output/step1_xxx/chunk_xxx.txt
'position' table definition (.cfg, .schema): /sps/lsst/users/elles/qserv_install_local/qserv_testdata/tasks/task_75/sph_partition/

## How to manage a queue in SQL
https://stackoverflow.com/questions/423111/whats-the-best-way-of-implementing-a-messaging-queue-table-in-mysql
