import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
from io import StringIO
import os
import time
import domo_python
import lib.cuddle_snowflake as cuddle_snowflake

def push_all_datasets_to_snowflake( domo_mapping_df, domo_client_id, domo_client_secret, connection ) :
    start_whole = time.time()

    for row in domo_mapping_df.itertuples():
        start_time = time.time()
        to_table_name = row[1]
        domo_dataset_id = row[2]
        domo_df = domo_python.domo_csv_to_dataframe( domo_dataset_id, domo_client_id, domo_client_secret )
        result = cuddle_snowflake.dataframe_to_snowflake( domo_df, to_table_name, connection )
        end_time = time.time()
        table_time = end_time - start_time
        print("Time for {table}: {time} seconds".format(time = table_time, table = to_table_name))

    end_whole = time.time()
    whole_time = (end_whole - start_whole)
    print("Total time: {time} seconds".format(time = whole_time))