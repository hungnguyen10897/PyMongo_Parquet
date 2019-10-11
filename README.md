# PyMongo_Parquet

The module uses  [pymongo](https://pypi.org/project/pymongo/) to interact with [MongoDB GridFS](https://docs.mongodb.com/manual/core/gridfs/) which stores parquet files. It can handle csv, parquet files or directly pandas DataFrame when using within another python script.

This project uses Python 3.6.7 64-bit.

## Objectives/Use case
#### Motivation

In our situation, we need to capture snapshots from a couple of large tables from our data lake every 5 minutes. These snapshots/tables, originally stored on a database of MSSQL Server, can easily exceeds 28MB in space usage for each of them accumulates quickly to a certain amount of table objects in the database which slows down the Server significantly, especially when querying on SSMS.

The snapshots are accessed and used as a whole, their per column values are mostly sparse and duplicated. We need a solution that is easily available without having to make too big a leap in our current tech stack.


#### Solution

Firstly, [Apache Parquet](https://parquet.apache.org/) (columnar storage) file format is a suitable compression method, being able to compress 28MB snapshot (reported on MSSQL Server) downto only 2MB parquet file. 



Second, having the parquet files available, there must be a way to effectively store and retrieve the files. These snapshots' data are never updated. The whole snapshots themselves are inserted and retrieved while only their metadata are queried.

For that, **MongoDB GridFS** is really good. **GridFS** separates the parquet files access into accessing the metadata (_id or name of the snapshot, uploadDate...) which can be added per user need and accessing the real underlying data. This allows for querying metadata without the burden of the actual data. **GridFS** eliminates the constraint of 16MB document size of **MongoDB** by dividing the input file into 255KB chunks. 

The idea is to turn all input snapshots into parquet files and then ingest them into **MongoDB GridFS** in the form of binary data. This is done using Python. Conversely, reading data out involves writing the binary data into a parquet file.



## Dependency
This module uses [pyarrow](https://pypi.org/project/pyarrow/) to deal with parquet files and [pymongo](https://pypi.org/project/pymongo/) to interact with MongoDDB GridFS.

```pip install pymongo```

```pip install pyarrow```

If use need to use MSSQL ingestion plugin:

```pip install numpy```

```pip install pandas```

```pip install pyodbc```

or using requirements.txt file from the repo

```pip install -r requirements.txt```


## How to use

This can be used either as a command line python script or as a module in another python script.

**Using as a command line python prograrm**  

To display a list of help for the operations and options.

```
python mongodb_gridfs_operator.py -h
usage: mongodb_gridfs_operator.py [-h] [-f CONFIGURATION]
                                  [-c CONNECTION_STRING] [-d DATABASE]
                                  [-b BUCKET] [-u USERNAME] [-pw PASSWORD]
                                  {find,export,ingest,delete,drop} ...

                        Python operator for MongoDB GridFS.


positional arguments:
  {find,export,ingest,delete,drop}
                        Operations: <operation> --help for additional help
    find                print and obtain a list of filenames in database.
    export              export files from MongodDB GridFS.
    ingest              ingest parquet/csv files in a directory into the
                        Database. If csv files are used for ingestion, all
                        columns are of type str.
    delete              delete collections from temporary Compass view.
    drop                permanently drop files from MongoDB GridFS.

optional arguments:
  -h, --help            show this help message and exit
  -f CONFIGURATION, --configuration CONFIGURATION
                        Path to configuration file containing connection
                        string, database name, GridFS bucket name, username
                        and password. If not provided, the program will try to
                        read from a 'config.cfg' file from the working
                        directory.
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
  -u USERNAME, --username USERNAME
                        Username to authenticate to MongoDB database. This
                        overrides the username in the configuration file
                        (--configuration)
  -pw PASSWORD, --password PASSWORD
                        Password corresponding to --username provided. This
                        overrides the password in the configuration file
                        (--configuration)
```

Operations are implemented as subcommands of the main script. For detail help of each operation call the subcommand with -h.

E.g show help for operation export:

```
python mongodb_gridfs_operator.py export -h

usage: mongodb_gridfs_operator.py export [-h] -e
                                         {csv,parquet,compass,df,mssql} -p
                                         PATTERN [-t TARGET_DIRECTORY]
                                         [-l LIMIT]

optional arguments:
  -h, --help            show this help message and exit
  -e {csv,parquet,compass,df,mssql}, --export_format {csv,parquet,compass,df,mssql}
                        exported file format: csv/parquet files are written to
                        disk, 'compass' will create a collection for each file
                        in MongoDB to view the data. 'df' will return a list
                        of tuple of type (DataFrame, filename). 'mssql'
                        exports the files to SQL Server whose connection is
                        specified in config.cfg section [MSSQL CONNECTION]
  -p PATTERN, --pattern PATTERN
                        a pattern according to which matching files are
                        exported.
  -t TARGET_DIRECTORY, --target_directory TARGET_DIRECTORY
                        destination directory to dump csv/parquet files.
  -l LIMIT, --limit LIMIT
                        number of files exported. If no limit is specified,
                        all collections matching the pattern (-p) are
                        exported.
```

<br/>

  To start using this script, a connection to MongoDB Server must be specified either via a configuration file (-f, --configuration) or by using -c, -d, -b, -u, -pw arguments. Without providing -f argument, the program looks for a 'config.cfg' file at the same directory with the script.

These arguments have to be specifed before a subcommand and its specific arguments.
<br/>
<br/>
  To list all files in this bucket.
```
python mongodb_gridfs_operator.py -f PATH/TO/CONFIG_FILE find
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

  To export files with names of pattern 'G4' into Compass for viewing using default connection
  
```
python mongodb_gridfs_operator.py export -e compass -p .*G4.*
```
MongoDB comes with [Compass](https://www.mongodb.com/products/compass), a visual/analytic tool to view your data. However, only the metadata makes sense in Compass since we store the actual data in form of binary of parquet files. The collection containing metadata is: your_database_name/your_bucket_name/files. In my case, myDB/fs/files:

![alt text](https://github.com/hungnguyen10897/PyMongo_Parquet/blob/master/Images/Capture0.PNG "Compass capture 1")
<br/>
<br/>
<br/>
<br/>
  To view these data, export action with (-e compass) will add these snapshots to the current MongoDB database as collections containing the actual data which can be meaningfully viewed through Compass.

![alt text](https://github.com/hungnguyen10897/PyMongo_Parquet/blob/master/Images/Capture1.PNG "Compass capture 2")
<br/>
<br/>
<br/>
<br/>
  To delete the first 10 snapshots on MongoDB which are only meant to be viewed on Compass

```
python mongodb_gridfs_operator.py delete -p .* -l 10
```

Now the database no longer contains the temporary snapshots.  

<br/>
<br/>
  To permanently drop files of certain pattern

```
python mongodb_gridfs_operator.py drop -p .*G0.*
```

<br/>
<br/>
  To ingest some csv files with a certain pattern in a directory, we pass that directory path to -s/ --source option.
  
```
python mongodb_gridfs_operator.py ingest -s /home/hung/csv_folder -p .*G0.*
```
<br/>
<br/>

**Using as a module in another Python prograrm**  

Other Python programs can utilize this project as module by accessing directly the operations. These are defined in Operator/operattions.py, which includes 5 functions for 5 operations (find, export, ingest, delete, drop).

The functionalities of the operations are generally the same. Certain arguments are required for all operations: 
  - db: a pymongo.database.Database object representing connection to a Database.
  - bucket: string of the name of the interacting bucket.

And there are operation-specific arguments.
  


## Further Improvement
- MongoDB Server authentication: This module accesses the server using only the Host Address, database name and bucket name of the GridFS. More type of authentication can be extended (username, password) and further checking for existence of database or bucket can be implemented. For example, another flag must be specify so that new databases/buckets are created if they do not exist already or else an error is thrown.

- In this setting, we only use standalone cluster/server as our data won't get out of control. But pymongo provides the ability to easily write highly available and scalable applications with the help of MongoDB properties like Replica set and Sharded cluster. More details [here](https://api.mongodb.com/python/current/examples/high_availability.html)

- Display metadata of the files

- Develop more functionalites to add more metadata to the files and ability to query the files according to metadata.


### Note: In this README, the terms "files", "snapshots" and "tables" are used interchangeably.










