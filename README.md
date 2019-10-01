# PyMongo_Parquet

The module uses  [pymongo](https://pypi.org/project/pymongo/) to interact with [MongoDB GridFS](https://docs.mongodb.com/manual/core/gridfs/) which stores parquet files. It can handle csv, parquet files or directly pandas DataFrame when using within another python script.


## Objectives/  Use case
#### Motivation

In our situation, we need to capture snapshots from a couple of large tables from our data lake every 5 minutes. These snapshots/tables, originally stored on a database of MSSQL Server, can easily exceeds 28MB in space usage for each of them accumulates quickly to a certain amount of table objects in the database which slows down the Server significantly, especially when querying on SSMS.

The snapshots are accessed and used as a whole, their per column values are mostly sparse and duplicated. We need a solution that is easily available without having to make too big a leap in our current tech stack.


#### Solution

Firstly, [Apache Parquet](https://parquet.apache.org/) (columnar storage) file format is a suitable compression method, being able to compress 28MB snapshot (reported on MSSQL Server) downto only 2MB parquet file. 



Second, having the parquet files available, there must be a way to effectively store and retrieve the files. These snapshots' data are never updated. The whole snapshots themselves are inserted and retrieved while only their metadata are queried.

For that, **MongoDB GridFS** is really good. GridFS separates the parquet files access into accessing the metadata (_id or name of the snapshot, uploadDate...) which can be added per user need and accessing the real underlying data. This allows for querying metadata without the burden of the actual data. **GridFS** eliminates the constraint of 16MB document size of **MongoDB** by dividing the input file into 255KB chunks. 

The idea is to turn all input snapshots into parquet files and then ingest them into MongoDB GridFS in the form of binary data. This is done using Python. Conversely, reading data out involves writing the binary data into a parquet file.



## Dependency
This module uses [pyarrow](https://pypi.org/project/pyarrow/) to deal with parquet files and [pymongo](https://pypi.org/project/pymongo/) to interact with MongoDDB GridFS.

```pip install pymongo```

```pip install pyarrow```

## How to use

This can be used either as a command line python script or as a module in another python script.



## Example Usage

## Further Improvement
- MongoDB Server authentication: This module accesses the server using only the Host Address, database name and bucket name of the GridFS. More type of autthentication can be extended (username, password), further checking for existence of database or bucket can be implemented. For example, another flag must be specify so that new databases/buckets are created if they do not exist already or else an error is thrown.

- In this setting, we only use standalone cluster/server as our data won't get out of control. But pymongo provides the ability to easily write highly available and scalable applications with the help of MongoDB properties like Replica set and Sharded cluster. More details [here](https://api.mongodb.com/python/current/examples/high_availability.html)










