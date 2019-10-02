import os
from gridfs import GridFS
import pyarrow as pa
import pyarrow.parquet as pq
import json
import re
import pandas as pd

def find(db, bucket, pattern='.*'):
    res = db[bucket].files.find({"_id" : {'$regex' : pattern}},projection = {"_id": 1})
    list_res = list(map( lambda x: list(x.values())[0].split('.')[0], res))
    for file in list_res:
        print("{:5}".format(str(i)+".") + file)
        i+=1
    return list_res

def export(db, bucket, export_format, pattern, target_directory = os.getcwd()):

    if export_format in ['parquet','csv'] and not os.path.exists(target_directory):
        raise OSError(f"Target directory '{target_directory}' not found.")
    if export_format in ['df', 'compass'] and not os.path.exists(target_directory):
        target_directory = os.getcwd()

    filenames = find(db, bucket, pattern)
    fs = GridFS(db, collection= bucket)
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
        fs = GridFS(db, collection= bucket)
        fs.delete(filename)

def ingest(db, bucket, source, pattern='.*', name_list=None):
    """

    If source is a directory, only the files in root directory are scanned and ingested.
    """
    fs = GridFS(db, collection= bucket)
    
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