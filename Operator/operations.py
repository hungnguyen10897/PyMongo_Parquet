import os
from gridfs import GridFS
import pyarrow as pa
import pyarrow.parquet as pq
import json
import re
import pandas as pd



def find(db, bucket, pattern='.*', limit=None, take_empty=True, sort='asc', print_output = True):
    """
        To find and print a set of filenames from MongoDB GridFS.
        Parameters
        ----------
            db: pymongo.database.Database 
                Connection to a Database from MongoDB
            bucket: str
                the name of the interacting bucket.
            pattern: str, optional
                a regex pattern to match file name.
            limit: int/None
                limit of the number of filenames listed/printed.
        
        Returns
        -------
            list_res: 
                a list of filenames matching argument pattern.
    """
    i = 1
    pipeline = [{'$match' : {"_id": {'$regex' : pattern}}}]

    if not take_empty:
        pipeline.append({'$match' : {"rows" :{'$gt': 0}}})

    if sort == 'asc':
        sort_order = 1
    elif sort == 'desc':
        sort_order = -1
    pipeline.append({'$sort' : {'_id' : sort_order}})

    if limit is not None:
        pipeline.append({'$limit' : int(limit)})
    
    pipeline.append({'$project' : {"_id" : 1}})
    res = db[bucket].files.aggregate(pipeline)
    list_res = list(map( lambda x: list(x.values())[0].split('.')[0], res))

    if print_output:
        if len(list_res) == 0:
            print("No files found.")
        for file in list_res:
            print("{:5}".format(str(i)+".") + file)
            i+=1

    return list_res

def export(db, bucket, export_format, pattern, target_directory = os.getcwd(), limit=None, take_empty = True, sort='asc', **kwargs):
    """
    Return:
        If export_format is 'df', a list of tuples of type (filename, DataFrame) are returned.
        Otherwise,  nothing is returned.
    """
    print(f"Exporting these snapshots from MongoDB GridFS {db.name}.{bucket}:")
    dest_db = ''
    if export_format == "mssql":
        export_format = 'df'
        dest_db = 'mssql'
        mssql_conn_str = kwargs['mssql_conn_str']
        database_name = kwargs['database_name']
        schema = kwargs['schema']
        concurrency = kwargs['concurrency']
        mssql_ingest = kwargs['ingest_function']

    if export_format in ['parquet','csv'] and not os.path.exists(target_directory):
        raise OSError(f"Target directory '{target_directory}' not found.")
    if export_format in ['df', 'compass'] and not os.path.exists(target_directory):
        target_directory = os.getcwd()

    filenames = find(db, bucket, pattern, limit= limit, take_empty= take_empty, sort= sort, print_output= False)
    fs = GridFS(db, collection= bucket)
    dfs = []
    i=1
    for filename in filenames:
        
        official_filename = os.path.join(target_directory, filename + '.parquet')
        b_content = fs.get(filename).read()
        with open(official_filename, 'wb') as f:
            f.write(b_content)

        table = pq.read_table(official_filename)
        df = table.to_pandas()

        #for use in another scrript
        if export_format == 'df':
            dfs.append(df)

########### Problem when df is Empty
        if export_format == 'compass':
            json_str = df.to_json(orient='records')
            db[filename].insert_many(json.loads(json_str))

        elif export_format == 'csv':
            with open(os.path.join(target_directory, filename + '.csv'), 'w') as f:
                f.write(df.to_csv(index = False))
        
        #delete parquet file
        if export_format != 'parquet':
            os.remove(official_filename)

        print("{:5}".format(str(i)+".") + f"{filename}")
        i+=1

    if export_format == "df":
        dfs = list(zip(filenames, dfs))
        if dest_db == '':
            return dfs
        elif dest_db == 'mssql':
            mssql_ingest(dfs, mssql_conn_str, database_name, schema, concurrency)


def delete(db, bucket, pattern, limit = None):
    filenames = find(db, bucket, pattern, limit= limit, print_output= False)
    print(f"Deleting these snapshots (only for viewing in MongoDB Compass) from Database {db.name}:")
    collections = db.list_collection_names()
    i = 1
    for filename in filenames:
        if filename in collections:
            db.drop_collection(filename)
            print("{:5}".format(str(i)+".") + f"{filename}")
            i+=1


def drop(db, bucket, pattern, limit = None):
    print(f"Permanently dropping these snapshots from MongoDB GridFS {db.name}.{bucket}:")
    filenames = find(db, bucket, pattern, limit = limit, print_output= False)
    i = 1
    for filename in filenames:
        fs = GridFS(db, collection= bucket)
        fs.delete(filename)
        print("{:5}".format(str(i)+".") + f"{filename}")
        i+=1

def ingest(db, bucket, source, pattern='.*', name_list=None):
    """

    If source is a directory, only the files in root directory are scanned and ingested.
    """
    
    fs = GridFS(db, collection= bucket)
    
    #if source is a list of pandas DataFrames
    if type(source) ==  list:
        print(f"Ingesting from a list of DataFrames- length: {len(source)}")
        try:
            if name_list is None or len(name_list) != len(source):
                raise Exception("List of names to be stored either is not provided or does not have the same length as the list of DataFrames.")

            i = 1
            for df,filename in zip(source, name_list):
                if type(df) != pd.core.frame.DataFrame:
                    raise Exception("Element of source list needs to be of type DataFrame")

                if fs.exists(_id=f'{filename}'):
                        print(f"Skipping DataFrame with name {filename}")
                else:
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, f'{filename}.parquet')
                    fs.put(open(f'{filename}.parquet', 'rb'), _id = f'{filename}', rows =  df.shape[0])
                    os.remove(f'{filename}.parquet')
                    print("{:5}".format(str(i)+".") + f"{filename}")
                    i+=1

        except  Exception as e:
            print(str(e))        

    #if source is a directory containing parquet/csv files
    elif type(source) == str:
        print(f"Ingesting files from a directory ('{source}'):")
        try:
            for root, _, files in os.walk(source):
                i = 1
                for file in files:
                    absolute_file = os.path.join(root, file)
                    filename = file.split('.')[0]
                    extension = file.split('.')[-1]

                    if not re.match(pattern, file) or extension not in ['parquet', 'csv']:
                        continue

                    if fs.exists(_id=f'{filename}'):
                        print(f"Skipping file {file}")
                    else:
                        if extension == 'parquet':
                            table = pq.read_table(absolute_file)
                            df = table.to_pandas()
                            with open(absolute_file, 'rb') as f:
                                fs.put(f, _id = f'{filename}', rows = df.shape[0]) 
                        
                        elif extension == 'csv':
                            df = pd.read_csv(absolute_file, dtype=str)
                            table = pa.Table.from_pandas(df)
                            pq.write_table(table, f'{filename}.parquet')
                            with open(f'{filename}.parquet', 'rb') as f:
                                fs.put(f, _id = f'{filename}', rows = df.shape[0])
                            os.remove(f'{filename}.parquet')

                        print("{:5}".format(str(i)+".") + f"{file}")
                        i+=1

                break       #only scan the root directory

        except Exception as e:
            print(str(e))
            
    else:
        print("Type of source is not applicable.")