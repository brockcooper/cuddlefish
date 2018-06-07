import pandas as pd
import snowflake.connector
import sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

def snowflake_connection ( account, username, password, database, schema, warehouse ) :
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
    

def dataframe_to_snowflake ( df, table, connection ) :
      df.to_sql(table, connection, if_exists='replace', chunksize=1000, index=False)
