import pyodbc
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import sys
import time
import numpy as np

def retry_connect(conn_str, max_attempt=5):
    attempt = 1
    while attempt < max_attempt:
        try:
            return pyodbc.connect(conn_str, autocommit=True)
        except Exception as e:
            print("{}".format(str(e)))
            print(
                "Waiting for 180 seconds before establishing new connection")
            time.sleep(180)
            print(
                "Establishing new connection for SQL Server")
            attempt += 1
            if attempt == max_attempt:
                sys.exit(0)
                print(
                    "Could not establish new connection for vendor SQL Server")

def ingest(package):
    df_package = package[0]
    table_name = df_package[0]
    df = df_package[1]

    dtypes = df.dtypes
    cols = list(map(lambda x: f'[{x}]',dtypes.index.to_list()))

    
    # Inferring data types:
    if df.empty:
        print(f"Skipping {table_name}, empty snapshot.")
    else:
        mssql_data_types = []
        for i in df.loc[0,:]:
            if type(i) == bytes:
                mssql_data_types.append("VARBINARY(max)")
            elif type(i) == np.float64:
                mssql_data_types.append("NUMERIC")
            else:
                mssql_data_types.append("VARCHAR(max)")
    cols_types = map(lambda x: ' '.join(x),zip(cols, mssql_data_types, ["NULL"]*len(cols)))

    info = package[1]
    conn_str = info[0]
    db_name = info[1]
    schema = info[2]

    conn = retry_connect(conn_str)

    #CREATE TABLE
    with conn.cursor() as cursor:
        create_table_query_ = create_table_query.format(db_name, schema, table_name, ', '.join(cols_types))
        cursor.execute(create_table_query_)

    #INSERT INTO TABLE
    with conn.cursor() as cursor:
        cursor.fast_executemany = True
        insert_query_ = insert_query.format(db_name, schema, table_name, ', '.join(cols), ', '.join(['?']*len(cols)))
        cursor.executemany(insert_query_, df.values.tolist())

    conn.close()
    print(f"Finish ingesting f{table_name} to MSSQL")


def mssql_ingest(dfs, conn_str, db_name, schema, concurrency = True):

#dfs are what returned from 'export' operation
#each thread takes care of some df from dfs

    if concurrency:
        max_workers = None      # number of processors on the machine * 5
    else:
        max_workers = 1

    info = (conn_str, db_name, schema)
    packages = zip(dfs,[info]*len(dfs))
    with ThreadPoolExecutor(max_workers= max_workers) as executor:
        executor.map(ingest, packages)


create_table_query = """
    USE {0};
    DROP TABLE IF EXISTS {1}.{2};

    SET ANSI_NULLS ON

    SET QUOTED_IDENTIFIER ON

    CREATE TABLE {1}.{2}(
        {3}
    ) ON [PRIMARY]

"""

insert_query = """
    INSERT INTO {0}.{1}.{2} ({3}) VALUES ({4})
"""
