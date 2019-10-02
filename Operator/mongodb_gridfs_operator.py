from pymongo import MongoClient
import gridfs

import os
import sys
import argparse

import configparser
import os

from actions import find, export, delete, drop, ingest

if __name__ == "__main__":
    ap = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                        description= """
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
                        """)

    ap.add_argument("-c", "--connection_string",
                    help="Connection string to MongoDB Server. This overrides the connection string in the configuration file (--configuration) if provided.")

    ap.add_argument("-d", "--database", 
                    help="Database name for the queries. This overrides the database name in the configuration file (--configuration) if provided.")

    ap.add_argument("-b", "--bucket",
                    help="GridFS Bucket (abstraction of a separate GridFS within a database) name in the database. This overrides the bucket name in the configuration file (--configuration) if provided.")

    ap.add_argument("--username",help="Username to authenticate to MongoDB database. This overrides the username in the configuration file (--configuration)")
 
    ap.add_argument("--password",help="Password corresponding to --username provided. This overrides the password in the configuration file (--configuration)")

    ap.add_argument("-f","--configuration",
                    help="Path to onfiguration file containing connection string and database name.")

    ap.add_argument("-a", "--action", choices=['find','export','delete', 'drop', 'ingest'], required= True, \
                    help="Action to perform on database.")

    ap.add_argument("-p", "--pattern", help="Specify pattern (Python regex) for actions 'find'/'export'/'drop'/'delete'. The regex used here is different from that used in Mongo shell and Compass.")

    ap.add_argument("-e", "--export_format", choices=['csv', 'parquet', 'compass', 'df'], 
                    help="Specify the format for actions 'export': csv, parquet, compass(to view file on MongoDB Compass)")
                
    ap.add_argument("-t", "--target_directory",
                    help="Specify the target directory to dump csv/parquet files for actions 'export', default is the module source directory.")

    ap.add_argument("-s", "--source", help="Specify source for action 'ingest'. Source is a directory containing parquet/csv files for ingestion.")

    
    args = vars(ap.parse_args())

    #arguments validation
    config_path = args['configuration']
    mongodb_conn_str_ = args['connection_string']
    db_name_ = args['database']
    bucket_ = args['bucket']
    action = args['action']
    username_ = args['username']
    password_ = args['password']

    if config_path is None:
        if mongodb_conn_str_ is None:
            print("No connection string specified. Provide either configuration file containing connection string, database name and bucket name (--configuration) or an explicit connection string (--connection_string).")
            sys.exit(0)
        if db_name_ is None:
            print("No database specified. Provide either configuration file containing connection string, database name and bucket name (--configuration) or an explicit database name (--database).")
            sys.exit(0)
        if bucket_ is None:
            print("No bucket specified. Provide either configuration file containing connection string, database name and bucket name (--configuration) or an explicit bucket name (--database).")
            sys.exit(0)
    else:
        config = configparser.ConfigParser()
        config.read(config_path)

        mongodb_conn_str = config['CONNECTION']['mongodb_conn_str'] if mongodb_conn_str_ is None else mongodb_conn_str_
        db_name = config['CONNECTION']['database_name'] if db_name_ is None else db_name_
        bucket = config['CONNECTION']['bucket_name'] if bucket_ is None else bucket_
        username = config['CONNECTION']['username'] if username_ is None else username_
        password = config['CONNECTION']['password'] if password_ is None else password_

    if action == 'export' and (args['export_format'] is None or args['pattern'] is None):
        print("When choosing 'export' action, provide -e option to specify the exported format , as well as -p option to specify a pattern.")
        sys.exit(0)
    if action == 'delete' and args['pattern'] is None:
        print("When choosing 'delete' action, provide -p option to specify a pattern.")
        sys.exit(0)
    if action == 'drop' and args['pattern'] is None:
        print("When choosing 'drop' action, provide -p option to specify a pattern.")
        sys.exit(0)
    if action == 'ingest' and args['source'] is None:
        print("When choosing 'ingest' action, provide -s option to specify source.")
        sys.exit(0)

    if username == "" or password == "":
        client = MongoClient(mongodb_conn_str)
    else:
        client = MongoClient(mongodb_conn_str, username = username, password = password)

    db = client[db_name]

    if action == 'find':
        pattern = args['pattern']
        if pattern is None:
            pattern = '.*'
        find(db, bucket, pattern)

    elif action == 'export':
        target_directory = args['target_directory']
        if target_directory is None:
            target_directory = os.getcwd()
        export_format = args['export_format']
        pattern = args['pattern']
        export(db, bucket, export_format, pattern, target_directory)

    elif action == 'delete':
        pattern = args['pattern']
        delete(db, bucket, pattern)

    elif action == 'drop':
        pattern = args['pattern']
        drop(db, bucket, pattern)

    elif action == 'ingest':
        source = args['source']
        pattern = args['pattern'] if args['pattern'] is not None else '.*'
        ingest(db, bucket, source, pattern)






