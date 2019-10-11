import argparse

def find(args):
    print("find called")

def export(args):
    print("export called")

def ingest(args):
    print("ingest called")

def delete(args):
    print("delete called")

def drop(args):
    print("drop called")


if __name__ == "__main__":

    ap = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                        description= """
                        Python operator for MongoDB GridFS. 
                        """)

    ap.add_argument("-f","--configuration", help="Path to configuration file containing connection string, database name, GridFS bucket name, username and password.")
    ap.add_argument("-c", "--connection_string",
                    help="Connection string to MongoDB Server. This overrides the connection string in the configuration file (--configuration) if provided.")

    ap.add_argument("-d", "--database", 
                    help="Database name for the queries. This overrides the database name in the configuration file (--configuration) if provided.")

    ap.add_argument("-b", "--bucket",
                    help="GridFS Bucket (abstraction of a separate GridFS within a database) name in the database. This overrides the bucket name in the configuration file (--configuration) if provided.")

    ap.add_argument("-u","--username",help="Username to authenticate to MongoDB database. This overrides the username in the configuration file (--configuration)")
 
    ap.add_argument("-p","--password",help="Password corresponding to --username provided. This overrides the password in the configuration file (--configuration)")

    subparsers = ap.add_subparsers(help = 'Operations: <operation> --help for additional help')

    parser_find = subparsers.add_parser("find", help = "print and obtain a list of filenames in database.")
    parser_find.add_argument("-p", "--pattern", help="a pattern according to which matching files are listed. If -p is not provided, all files will be listed.")
    parser_find.add_argument("-l", "--limit", type=int, help="number of files printed/listed. If no limit is specified , all files matching the pattern (-p) are printed/listed.")
    parser_find.set_defaults(func = find)

    
    parser_export = subparsers.add_parser("export", help ="export files from MongodDB GridFS.")
    parser_export.add_argument("-e", "--export_format", choices= ['csv', 'parquet', 'compass', 'df', 'mssql'], required=True, help ="exported file format: csv/parquet files are written to disk, 'compass' will create a collection for each \
        file in MongoDB to view the data. 'df' will return a list of tuple of type (DataFrame, filename). 'mssql' exports the files to SQL Server whose connection is specified in config.cfg section [MSSQL CONNECTION]")
    parser_export.add_argument("-p", "--pattern", required= True, help="a pattern according to which matching files are exported.")
    parser_export.add_argument("-t", "--target_directory", help="destination directory to dump csv/parquet files.")
    parser_export.add_argument("-l", "--limit", type=int, help="number of files exported. If no limit is specified, all collections matching the pattern (-p) are exported.")
    parser_export.set_defaults(func = export)
   

    parser_ingest = subparsers.add_parser("ingest", help = "ingest parquet/csv files in a directory into the Database. If csv files are used for ingestion, all columns are of type str.")
    parser_ingest.add_argument("-s","--source",required=True, help="a directory containing the to-be-ingested files.")
    parser_ingest.add_argument("-p", "--pattern", help="a pattern according to which matching files are ingested. If -p option is not provided, all parquet/csv files will be ingested.")
    parser_ingest.set_defaults(func = ingest)

    parser_delete = subparsers.add_parser("delete", help = "delete collections from temporary Compass view.")
    parser_delete.add_argument("-p", "--pattern", required= True, help="a pattern according to which matching collections are deleted.")
    parser_delete.add_argument("-l","--limit", type=int, help="number of collections deleted. If no limit is specified, all collections matching the pattern (-p) are deleted.")
    parser_delete.set_defaults(func = delete)

    parser_drop = subparsers.add_parser("drop", help = "permanently drop files from MongoDB GridFS.")
    parser_drop.add_argument("-p", "--pattern", required= True, help="a pattern according to which matching files are dropped.")
    parser_drop.add_argument("-l","--limit", type=int, help="number of files dropped. If no limit is specified, all files matching the pattern (-p) are dropped.")
    parser_drop.set_defaults(func = drop)
   
    args = vars(ap.parse_args())
    print(args)

    if 'func' not in args:  #No subcommand is chosen.
        print("NO SUBCOMMAND")
    else:
        args['func'](1)


    # # Connection-related arguments validation
    # config_path = args['configuration']
    # mongodb_conn_str_ = args['connection_string']
    # db_name_ = args['database']
    # bucket_ = args['bucket']
    # operation = args['operation']
    # username_ = args['username']
    # password_ = args['password']