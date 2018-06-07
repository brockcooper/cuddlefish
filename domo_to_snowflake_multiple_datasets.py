import numpy as np
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
from io import StringIO
import os
import snowflake.connector
import sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import time

# Please change the variables in the following section as they apply to your specific use case
#############################################################

# Domo Variables from developer.domo.com
domo_client_id = "c28f1d02-5756-4613-98fd-75620f0d4a9b"
domo_client_secret = "83906d61b5481e5ace67fb01eb12227cf7af03f1619afa3b9ccefbfa06cdedca"

# Domo Dataset ID of the Domo to Snowflake Mapping File
# The mapping file should just be two columns:
# Column 1) Snowflake Table Name (this will be the name of the table we will create in snowflake. Needs to be 1 word and all lowercase)
# Column 2) Domo dataset ID (This will be the dataset id of the data that will be inserted into that table)
domo_dataset_id = "b50218b1-8080-4a47-b220-d9f94f07a75d"

# Setting your Snowflake account information
snowflake_account = 'bigsquid'
snowflake_username = 'chilton'
snowflake_password = 'Mossback030'
snowflake_database = 'STITCH_DATABASE'
snowflake_schema = 'SALESFORCE'
snowflake_warehouse = 'STITCH_WAREHOUSE'

#############################################################

### Create necessary functions

# Domo API Functions
def getAccessToken( clientId, clientSecret ) :
    response = requests.get("https://api.domo.com/oauth/token?grant_type=client_credentials&scope=data", auth = HTTPBasicAuth(clientId, clientSecret))
    j = json.loads(response.content.decode("utf-8"))
    return j["access_token"]

def exportData ( datasetId, access_token  ) :
    httpHeaders = {'content-type': 'application/json',
                        'Authorization' : 'bearer ' + access_token}
    response = requests.get('https://api.domo.com/v1/datasets/' + datasetId + '/data?includeHeader=true', headers = httpHeaders)
    return response.text

def domoCSVToDataframe ( domoDatasetId, domoClientId, domoClientSecret ) :
    # Import CSV and rename columns
    csv = exportData(domoDatasetId, getAccessToken( domoClientId, domoClientSecret ))
    forPandas = StringIO(csv)
    df = pd.read_csv(forPandas)
    return df

# Snowflake Functions
def snowflakeConnection ( account, username, password, database, schema, warehouse ) :
    engine = create_engine(URL(
        account = account,
        user = username,
        password = password,
        database = database,
        schema = schema,
        warehouse = warehouse,
        numpy=True
    ))

    return engine.connect()

def dfToSnowflake ( df, table, connection ) :
    df.to_sql(table, connection, if_exists='replace', chunksize=1000, index=False)
    result = pd.read_sql_query("SELECT * FROM {tablename}".format(tablename = table), connection)
    return result

def push_all_datasets_to_snowflake( domo_mapping_df, domo_client_id, domo_client_secret, snowflake_connection ) :
    start_whole = time.time()

    for row in domo_mapping_df.itertuples():
        start_time = time.time()
        to_snowflake_table_name = row[1]
        domo_dataset_id = row[2]
        domo_df = domoCSVToDataframe ( domo_dataset_id, domo_client_id, domo_client_secret )
        result = dfToSnowflake ( domo_df, to_snowflake_table_name, snowflake_connection )
        end_time = time.time()
        table_time = end_time - start_time
        print("Time for {table}: {time} seconds".format(time = table_time, table = to_snowflake_table_name))

    end_whole = time.time()
    whole_time = (end_whole - start_whole)
    print("Total time: {time} seconds".format(time = whole_time))

def automate_domo_to_snowflake (sf_account, sf_username, sf_password, sf_database, sf_schema, sf_warehouse,
                               domo_dataset_id, domo_client_id, domo_client_secret):

    # Get Domo to Snowflake Mapping File
    df = domoCSVToDataframe ( domo_dataset_id, domo_client_id, domo_client_secret )

    # Start Snowflake Connection
    connection = snowflakeConnection ( sf_account, sf_username, sf_password, sf_database, sf_schema, sf_warehouse )

    # Push all datasets to Snowflake
    push_all_datasets_to_snowflake( df, domo_client_id, domo_client_secret, connection )

    connection.close()

# Trigger the automation to happen

automate_domo_to_snowflake(snowflake_account, snowflake_username, snowflake_password, snowflake_database,
                           snowflake_schema, snowflake_warehouse,
                           domo_dataset_id, domo_client_id, domo_client_secret)