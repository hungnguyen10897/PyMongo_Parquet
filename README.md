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

**Using as a command line python script**  

To display a list of help for arguments and options.

```
python mongodb_gridfs_operator.py
usage: mongodb_gridfs_operator.py [-h] [-c CONNECTION_STRING] [-d DATABASE]
                                  [-b BUCKET] [-f CONFIGURATION] -a
                                  {find,export,delete,drop,ingest}
                                  [-p PATTERN] [-e {csv,parquet,compass,df}]
                                  [-t TARGET_DIRECTORY] [-s SOURCE]

                        Query tool for MongoDB GridFS.
                        -------------------------------------
                        Specify action to perform with argument -a (--action):
                            - find: to print/get a list of filenames in database. Optionally, provide -p option to specify a pattern according to which matching files are listed. If -p is not provided, all files will be listed.

                            - export: export files from MongodDB GridFS. Upon choosing this action, provide -e option to specify exported file format, as well as -p option to specify a pattern  according to which matching files are exported
                            Optionally, provide -t option to specify the destination directory to dump csv/parquet files.

                            - delete: delete tables from temporary Compass view. Upon choosing this action, provide -p option to specify a pattern according to which matching files are deleted.

                            - drop: permanently drop files from MongoDB GridFS. Upon choosing this action, provide -p option to specify a pattern according to which matching files are dropped.

                            - ingest: ingest a list of parquet/csv files in a directory into the Database. Upon choosing this action, provide -s option to specify a directory containing the files.
                            An optional argument -p can be provided to specify a pattern so that only parquet/csv files matching the pattern will be ingested. All parquet/csv files will be ingested
                            if -p option is not provided. If csv files are used for ingestion, all columns are of type str.


optional arguments:
  -h, --help            show this help message and exit
  -c CONNECTION_STRING, --connection_string CONNECTION_STRING
                        Connection string to MongoDB Server. This overrides
                        the connection string in the configuration file
                        (--configuration) if provided.
  -d DATABASE, --database DATABASE
                        Database name for the queries. This overrides the
                        database name in the configuration file
                        (--configuration) if provided.
  -b BUCKET, --bucket BUCKET
                        GridFS Bucket (abstraction of a separate GridFS within
                        a database) name in the database. This overrides the
                        bucket name in the configuration file
                        (--configuration) if provided.
  -f CONFIGURATION, --configuration CONFIGURATION
                        Path to onfiguration file containing connection string
                        and database name.
  -a {find,export,delete,drop,ingest}, --action {find,export,delete,drop,ingest}
                        Action to perform on database.
  -p PATTERN, --pattern PATTERN
                        Specify pattern (Python regex) for actions
                        'find'/'export'/'drop'/'delete'. The regex used here
                        is different from that used in Mongo shell and
                        Compass.
  -e {csv,parquet,compass,df}, --export_format {csv,parquet,compass,df}
                        Specify the format for actions 'export': csv, parquet,
                        compass(to view file on MongoDB Compass)
  -t TARGET_DIRECTORY, --target_directory TARGET_DIRECTORY
                        Specify the target directory to dump csv/parquet files
                        for actions 'export', default is the module source
                        directory.
  -s SOURCE, --source SOURCE
                        Specify source for action 'ingest'. Source is a
                        directory containing parquet/csv files for ingestion.
```

To start using this script, a connection to MongoDB Server must be specified either at a configuration file and provide the file with -f or by using -c, -d, -b options.

Furthermore, an action (-a) is chosen and provided with suitable options and arguments (-p, -s, -t, -e)
<br/>
<br/>
  To list all files in this bucket
```
python mongodb_gridfs_operator.py -f config.cfg -a find
1.   REPL_FCO_DOC_HEAD_G4_1569805944
2.   REPL_FCO_DOC_POS_G4_1569805983
3.   REPL_LQUA_G0_1569805641
4.   REPL_LQUA_G4_1569805653
5.   REPL_MAKT_G0_1569805891
6.   REPL_MAKT_G4_1569805925
7.   REPL_MARC_G4_1569805713
8.   REPL_MBEW_G4_1569805772
9.   REPL_MSEG_G0_1569806673
10.  REPL_MSEG_G4_1569806025
11.  REPL_TCURR_G4_1569805874
12.  REPL_VBUP_G4_1569805604
```
<br/>
<br/>
  To export files with names of pattern 'G4' into Compass for viewing
```
python mongodb_gridfs_operator.py -f config.cfg -a export -e compass -p .*G4.*
```
MongoDB comes with Compass, a visual tool to view your data. However, only the metadata makes sense in Compass since we store the actual data in form of binary of parquet files. The collection containing metadata is: your_database_name / your_bucket_name / files. In my case, myDB/fs/files:

/////IMAGE HERE
<br/>
<br/>
  To view these data, export action with (-e compass) will add these snapshots to the current MongoDB database as collections containing the actual data which can be meaningfully viewed through Compass.

/////IMAGE HERE
<br/>
<br/>
  To delete all these temporary snapshots on MongoDB which are only meant to be viewed on Compass

```
python -f config.cfg -a delete -p .*
```

Now the database no longer contains the temporary snapshots.  

<br/>
<br/>
  To permanently drop files of certain pattern

```
python -f config.cfg -a drop -p .*G0.*
```

<br/>
<br/>
  To ingest some csv files with a certain pattern in a directory, we pass that directory path to -s/ --source option.
  
```
python -f config.cfg -a ingest -s /home/hung/csv_folder -p .*G0.*
```

## Further Improvement
- MongoDB Server authentication: This module accesses the server using only the Host Address, database name and bucket name of the GridFS. More type of authentication can be extended (username, password) and further checking for existence of database or bucket can be implemented. For example, another flag must be specify so that new databases/buckets are created if they do not exist already or else an error is thrown.

- In this setting, we only use standalone cluster/server as our data won't get out of control. But pymongo provides the ability to easily write highly available and scalable applications with the help of MongoDB properties like Replica set and Sharded cluster. More details [here](https://api.mongodb.com/python/current/examples/high_availability.html)

- Display metadata of the files

- Develop more functionalites to add more metadata to the files and ability to query the files according to metadata.

# Note: In this README, files/ snapshots/ tables are used interchangeably.










