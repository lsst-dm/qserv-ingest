########################
Set up ingest input data
########################

Example
=======

This `example for input data <https://github.com/lsst-dm/qserv-ingest/tree/tickets/DM-24587/data/example_db>`__ 
is used by `Qserv ingest continuous integration process <https://travis-ci.com/github/lsst-dm/qserv-ingest>`__.


input data
==========

Input data is produced by `Qserv partitioner <https://github.com/lsst/partition>`__ (i.e. `sph-partition`) and is made of multiples `*.csv` files.
Each of these files contains a part of a chunk for a given database and table,
as shown in `this example <https://github.com/lsst-dm/qserv-ingest/blob/tickets/DM-24587/data/example_db/step1_1/position/chunk_57866.txt>`__.
Relation between an input data file and its related table and database is available inside `metadata.json`, detailed below. 

Metadata
========

Metadata files below describe input data and are required by `qserv-ingest`:

- `metadata.json`: contain the name of the files describing the database, the tables, the indexes, and also the relative path to the chunk files.
  Chunk files can be located in multiples directories.

  .. code:: json

    {
    "database":"test101.json",
    "tables":[
        {
            "schema":"director_table.json",
            "indexes":[
                "idx_director.json"
            ],
            "data":[
                {
                "directory":"director/dir1",
                "chunks":[
                    57866,
                    57867
                ]
                },
                {
                "directory":"director/dir2",
                "chunks":[
                    57868
                ]
                }
            ]
        },
        {
            "schema":"partitioned_table.json",
            "indexes":[
                "idx_partitioned.json"
            ],
            "data":[
                {
                "directory":"partitioned/dir1",
                "chunks":[
                    57866,
                    57867
                ]
                },
                {
                "directory":"partitioned/dir2",
                "chunks":[
                    57868
                ]
                }
            ]
        }
        ]
    }

- `<database_name>.json`: describe the database to register inside the replication service and where the data will be ingested,
  its format is described in `replication service documentation <https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion1)-RegisteringanewdatabaseinQserv>`__.
- `<table_name>.json`: each of these files describes a table to register inside the replication service and where the data will be ingested,
  its format is described in `replication service documentation <https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion1)-Registeringatable>`__.
- `<table_index>.json`:each of these files describes an index to create for a given set of chunk tables,
  its format is described in `replication service documentation <https://confluence.lsstcorp.org/display/DM/Managing+indexes#Managingindexes-Request>`__.
