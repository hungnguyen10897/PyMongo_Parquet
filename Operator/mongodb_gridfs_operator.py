import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

import pymongo
from pymongo import MongoClient
import gridfs

import pandas as pd
import os
import sys
import time
import argparse
import re
import json
import configparser
import os

def find(db, bucket, pattern='.*'):
    res = db.fs.files.find({"_id" : {'$regex' : pattern}},projection = {"_id": 1})
    list_res = list(map( lambda x: list(x.values())[0].split('.')[0], res))
    for file in list_res:
        print(file)
    return list_res

def export(db, bucket, export_format, pattern, target_directory = os.getcwd()):

    if export_format in ['parquet','csv'] and not os.path.exists(target_directory):
        raise OSError(f"Target directory '{target_directory}' not found.")
    if export_format in ['df', 'compass'] and not os.path.exists(target_directory):
        target_directory = os.getcwd()

    filenames = find(db, bucket, pattern)
    fs = gridfs.GridFS(db, collection= bucket)
    for filename in filenames:
        official_filename = os.path.join(target_directory, filename + '.parquet')

        b_content = fs.get(filename).read()

        with open(official_filename, 'wb') as f:
            f.write(b_content)

        if export_format == 'parquet':
            return

        else:
            table = pq.read_table(official_filename)
            df = table.to_pandas()

            #for use in another scrript
            if export_format == 'df':
                os.remove(official_filename)
                return df

            if export_format == 'compass':
                json_str = df.to_json(orient='records')
                db[filename].insert_many(json.loads(json_str))

            elif export_format == 'csv':
                with open(os.path.join(target_directory, filename + '.csv'), 'w') as f:
                    f.write(df.to_csv(index = False))
            
            #delete parquet file
            os.remove(official_filename)

def delete(db, bucket, pattern):
    filenames = find(db, bucket, pattern)
    for filename in filenames:
        db.drop_collection(filename)

def drop(db, bucket, pattern):
    filenames = find(db, bucket, pattern)
    for filename in filenames:
        fs = gridfs.GridFS(db, collection= bucket)
        fs.delete(filename)

def ingest(db, bucket, source, pattern='.*', name_list=None):
    """

    If source is a directory, only the files in root directory are scanned and ingested.
    """
    fs = gridfs.GridFS(db, collection= bucket)
    
    #if source is a list of pandas DataFrames
    if type(source) ==  list:
        try:
            if name_list is None or len(name_list) != len(source):
                raise Exception("List of names to be stored either is not provided or does not have the same length as the list of DataFrames.")

            for df,filename in zip(source, name_list):
                if type(df) != pd.core.frame.DataFrame:
                    raise Exception("Element of source list needs to be of type DataFrame")

                table = pa.Table.from_pandas(df)
                pq.write_table(table, f'{filename}.parquet')
                fs.put(open(f'{filename}.parquet', 'rb'), _id = f'{filename}')
                os.remove(f'{filename}.parquet')

        except  Exception as e:
            print(str(e))        

    #if source is a directory containing parquet/csv files
    elif type(source) == str:
        try:
            for root, _, files in os.walk(source):
                for file in files:
                    
                    absolute_file = os.path.join(root, file)
                    filename = file.split('.')[0]
                    extension = file.split('.')[-1]

                    if not re.match(pattern, file) or extension not in ['parquet', 'csv']:
                        continue

                    print(file)
                    if extension == 'parquet':
                        with open(absolute_file, 'rb') as f:
                            fs.put(f, _id = f'{filename}') 
                    
                    elif extension == 'csv':
                        df = pd.read_csv(absolute_file, dtype=str)
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, f'{filename}.parquet')
                        with open(f'{filename}.parquet', 'rb') as f:
                            fs.put(f, _id = f'{filename}')
                        os.remove(f'{filename}.parquet')

                break       #only scan the root directory

        except Exception as e:
            print(str(e))
            
    else:
        print("Type of source is not applicable.")

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

    if config_path is None:
        if mongodb_conn_str_ is None:
            print("No connection string specified. Provide either configuration file containing connection string, database name and bucket name (--configuration) or an explicit connection string (--connection_string).")
            sys.exit(1)
        if db_name_ is None:
            print("No database specified. Provide either configuration file containing connection string, database name and bucket name (--configuration) or an explicit database name (--database).")
            sys.exit(1)
        if bucket_ is None:
            print("No bucket specified. Provide either configuration file containing connection string, database name and bucket name (--configuration) or an explicit bucket name (--database).")
    else:
        config = configparser.ConfigParser()
        config.read(config_path)

        mongodb_conn_str = config['CONNECTION']['mongodb_conn_str'] if mongodb_conn_str_ is None else mongodb_conn_str_
        db_name = config['CONNECTION']['database_name'] if db_name_ is None else db_name_
        bucket = config['CONNECTION']['bucket_name'] if bucket_ is None else bucket_

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


    client = MongoClient(mongodb_conn_str)
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






