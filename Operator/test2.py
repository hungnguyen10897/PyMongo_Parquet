import argparse


if __name__ == "__main__":

    ap = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                        description= """
                        Query tool for MongoDB GridFS. 


                        """)

    subparsers = ap.add_subparsers(help = 'sub-command help', description= """                        Specify operation to perform with argument -o (--operation):
                            - find: to print/get a list of filenames in database. Optionally, provide -p option to specify a pattern according to which matching files are listed. If -p is not provided, all files will be listed. 

                            - export: export files from MongodDB GridFS. Upon choosing this operation, provide -e option to specify exported file format, as well as -p option to specify a pattern  according to which matching files are exported
                            Optionally, provide -t option to specify the destination directory to dump csv/parquet files.

                            - delete: delete tables from temporary Compass view. Upon choosing this operation, provide -p option to specify a pattern according to which matching files are deleted.

                            - drop: permanently drop files from MongoDB GridFS. Upon choosing this operation, provide -p option to specify a pattern according to which matching files are dropped.

                            - ingest: ingest a list of parquet/csv files in a directory into the Database. Upon choosing this operation, provide -s option to specify a directory containing the files. 
                            An optional argument -p can be provided to specify a pattern so that only parquet/csv files matching the pattern will be ingested. All parquet/csv files will be ingested 
                            if -p option is not provided. If csv files are used for ingestion, all columns are of type str.""")

    parser_find = subparsers.add_parser("find", help = "a help")
    parser_export = subparsers.add_parser("export", help = "a help")

    args = vars(ap.parse_args())



    # ap.add_argument("-c", "--connection_string",
    #                 help="Connection string to MongoDB Server. This overrides the connection string in the configuration file (--configuration) if provided.")

    # ap.add_argument("-d", "--database", 
    #                 help="Database name for the queries. This overrides the database name in the configuration file (--configuration) if provided.")

    # ap.add_argument("-b", "--bucket",
    #                 help="GridFS Bucket (abstraction of a separate GridFS within a database) name in the database. This overrides the bucket name in the configuration file (--configuration) if provided.")

    # ap.add_argument("--username",help="Username to authenticate to MongoDB database. This overrides the username in the configuration file (--configuration)")
 
    # ap.add_argument("--password",help="Password corresponding to --username provided. This overrides the password in the configuration file (--configuration)")

    # ap.add_argument("-f","--configuration",
    #                 help="Path to configuration file containing connection string and database name.")

    # ap.add_argument("-o", "--operation", choices=['find','export','delete', 'drop', 'ingest'], required= True, \
    #                 help="operation to perform on database.")

    # ap.add_argument("-p", "--pattern", help="Specify pattern (Python regex) for operations 'find'/'export'/'drop'/'delete'. The regex used here is different from that used in Mongo shell and Compass.")

    # ap.add_argument("-e", "--export_format", choices=['csv', 'parquet', 'compass', 'df', 'mssql'], 
    #                 help="Specify the format for operations 'export': csv, parquet, compass (to view file on MongoDB Compass), df (a list of dfs and filenames, to be used in other script) or mssql (create table in MSSQL Server)")
                
    # ap.add_argument("-t", "--target_directory",
    #                 help="Specify the target directory to dump csv/parquet files for operations 'export', default is the module source directory.")

    # ap.add_argument("-s", "--source", help="Specify source for operation 'ingest'. Source is a directory containing parquet/csv files for ingestion.")

    # ap.add_argument("-l", "--limit", type= int, help="Specify the number of affected files for operations: 'find', 'export', 'delete', 'drop'. If no limit was specified, all files are affected.")
    
    # args = vars(ap.parse_args())

    # # Connection-related arguments validation
    # config_path = args['configuration']
    # mongodb_conn_str_ = args['connection_string']
    # db_name_ = args['database']
    # bucket_ = args['bucket']
    # operation = args['operation']
    # username_ = args['username']
    # password_ = args['password']