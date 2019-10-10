from pymongo import MongoClient
import gridfs

import os
import sys
import argparse
import configparser

from operations import find, export, delete, drop, ingest

if __name__ == "__main__":

    ap = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                        description= """
                        Python operator for MongoDB GridFS. 
                        """)

    ap.add_argument("-f","--configuration", help="Path to configuration file containing connection string, database name, GridFS bucket name, username and password. If not provided, the program will try to read \
        from a 'config.cfg' file from the working directory.")
    ap.add_argument("-c", "--connection_string",
                    help="Connection string to MongoDB Server. This overrides the connection string in the configuration file (--configuration) if provided.")

    ap.add_argument("-d", "--database", 
                    help="Database name for the queries. This overrides the database name in the configuration file (--configuration) if provided.")

    ap.add_argument("-b", "--bucket",
                    help="GridFS Bucket (abstraction of a separate GridFS within a database) name in the database. This overrides the bucket name in the configuration file (--configuration) if provided.")

    ap.add_argument("-u","--username",help="Username to authenticate to MongoDB database. This overrides the username in the configuration file (--configuration)")

    ap.add_argument("-pw","--password",help="Password corresponding to --username provided. This overrides the password in the configuration file (--configuration)")

    subparsers = ap.add_subparsers(help = 'Operations: <operation> --help for additional help')

    parser_find = subparsers.add_parser("find", help = "print and obtain a list of filenames in database.")
    parser_find.add_argument("-p", "--pattern", default='.*' ,help="a pattern according to which matching files are listed. If -p is not provided, all files will be listed.")
    parser_find.add_argument("-l", "--limit", type=int, help="number of files printed/listed. If no limit is specified , all files matching the pattern (-p) are printed/listed.")
    parser_find.set_defaults(operation = 'find')

    
    parser_export = subparsers.add_parser("export", help ="export files from MongodDB GridFS.")
    parser_export.add_argument("-e", "--export_format", choices= ['csv', 'parquet', 'compass', 'df', 'mssql'], required=True, help ="exported file format: csv/parquet files are written to disk, 'compass' will create a collection for each \
        file in MongoDB to view the data. 'df' will return a list of tuple of type (DataFrame, filename). 'mssql' exports the files to SQL Server whose connection is specified in config.cfg section [MSSQL CONNECTION]")
    parser_export.add_argument("-p", "--pattern", required= True, help="a pattern according to which matching files are exported.")
    parser_export.add_argument("-t", "--target_directory", default= os.getcwd(), help="destination directory to dump csv/parquet files.")
    parser_export.add_argument("-l", "--limit", type=int, help="number of files exported. If no limit is specified, all collections matching the pattern (-p) are exported.")
    parser_export.set_defaults(operation = 'export')
   

    parser_ingest = subparsers.add_parser("ingest", help = "ingest parquet/csv files in a directory into the Database. If csv files are used for ingestion, all columns are of type str.")
    parser_ingest.add_argument("-s","--source",required=True, help="a directory containing the to-be-ingested files.")
    parser_ingest.add_argument("-p", "--pattern", default= '.*', help="a pattern according to which matching files are ingested. If -p option is not provided, all parquet/csv files will be ingested.")
    parser_ingest.set_defaults(operation = 'ingest')

    parser_delete = subparsers.add_parser("delete", help = "delete collections from temporary Compass view.")
    parser_delete.add_argument("-p", "--pattern", required= True, help="a pattern according to which matching collections are deleted.")
    parser_delete.add_argument("-l","--limit", type=int, help="number of collections deleted. If no limit is specified, all collections matching the pattern (-p) are deleted.")
    parser_delete.set_defaults(operation = 'delete')

    parser_drop = subparsers.add_parser("drop", help = "permanently drop files from MongoDB GridFS.")
    parser_drop.add_argument("-p", "--pattern", required= True, help="a pattern according to which matching files are dropped.")
    parser_drop.add_argument("-l","--limit", type=int, help="number of files dropped. If no limit is specified, all files matching the pattern (-p) are dropped.")
    parser_drop.set_defaults(operation = 'drop')
   
    args = vars(ap.parse_args())
    
    if 'operation' not in args:  #No subcommand is chosen.
        print("No operation specified: choose an operation {find, export, ingest, delete, drop}.")
        print("Call --help for more help or <operation> --help for detail help of each operation.")
        sys.exit(0)

    operation = args['operation']

    # Connection-related arguments validation
    config_path = args['configuration']
    mongodb_conn_str_ = args['connection_string']
    db_name_ = args['database']
    bucket_ = args['bucket']
    username_ = args['username']
    password_ = args['password']

    if config_path is None:
        config_path = os.path.join(os.getcwd(), 'config.cfg')

    if not os.path.exists(config_path):
        
        if mongodb_conn_str_ is None:
            print("No configuration file found.")
            print("No connection string specified. Provide either configuration file (-f, --configuration) or an explicit connection string (--connection_string).")
            sys.exit(0)
        if db_name_ is None:
            print("No configuration file found.")
            print("No database specified. Provide either configuration file containing connection string, database name and bucket name (-f, --configuration) or an explicit database name (--database).")
            sys.exit(0)
        if bucket_ is None:
            print("No configuration file found.")
            print("No bucket specified. Provide either configuration file (-f,  --configuration) or an explicit bucket name (--bucket).")
            sys.exit(0)


    else:
        config = configparser.ConfigParser()
        config.read(config_path)
        mongodb_conn_str = config['CONNECTION']['mongodb_conn_str'] if mongodb_conn_str_ is None else mongodb_conn_str_
        db_name = config['CONNECTION']['database_name'] if db_name_ is None else db_name_
        bucket = config['CONNECTION']['bucket_name'] if bucket_ is None else bucket_
        username = config['CONNECTION']['username'] if username_ is None else username_
        password = config['CONNECTION']['password'] if password_ is None else password_

    # Connecting to MongoDB Server
    if username == "" or password == "":
        client = MongoClient(mongodb_conn_str)
    else:
        client = MongoClient(mongodb_conn_str, username = username, password = password)

    db = client[db_name]



    # General operation-related arguments validation
    pattern = args['pattern']

    # Specific operation-related arguments validaton and processing
    if operation == 'find':
        limit = args['limit']
        find(db, bucket, pattern, limit= limit)

    elif operation == 'export':
        export_format = args['export_format']
        target_directory = args['target_directory']
        limit = args['limit']
        db_args = {}
        if export_format == 'mssql':
            from database_ingestion_pluggins.mssql_ingestion import mssql_ingest
            db_args =   {
                    "mssql_conn_str" : config['MSSQL_INGESTION']['mssql_conn_str'],
                    "database_name" : config['MSSQL_INGESTION']['database_name'],
                    "schema" : config['MSSQL_INGESTION']['schema'],
                    "concurrency" : True if config['MSSQL_INGESTION']['concurrency'] == 'True' else False,
                    "ingest_function" : mssql_ingest
                    }
        export(db, bucket, export_format, pattern, target_directory, limit= limit, **db_args)

    elif operation == 'delete':
        limit = args['limit']
        delete(db, bucket, pattern, limit = limit)

    elif operation == 'drop':
        limit = args['limit']
        drop(db, bucket, pattern, limit = limit)

    elif operation == 'ingest':
        source = args['source']
        ingest(db, bucket, source, pattern)