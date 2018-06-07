import lib.domo_to_snowflake
import lib.cuddle_snowflake
import domo_python
import yaml

def automate_domo_to_snowflake (dw_account, dw_username, dw_password, dw_database, dw_schema, dw_warehouse,
                               domo_dataset_id, domo_client_id, domo_client_secret):

    # Get Domo to Snowflake Mapping File
    df = domo_python.domo_csv_to_dataframe ( domo_dataset_id, domo_client_id, domo_client_secret )

    # Start Snowflake Connection
    connection = lib.cuddle_snowflake.snowflake_connection ( dw_account, dw_username, dw_password, dw_database, dw_schema, dw_warehouse )

    # Push all datasets to Snowflake
    lib.domo_to_snowflake.push_all_datasets_to_snowflake( df, domo_client_id, domo_client_secret, connection )

    connection.close()

domo_auth = yaml.load(open('../configure/domo_auth.yaml', 'r'))
snowflake_auth = yaml.load(open('../configure/snowflake_auth.yaml', 'r'))

snowflake_account = snowflake_auth['snowflake_account']
snowflake_username = snowflake_auth['snowflake_username']
snowflake_password = snowflake_auth['snowflake_password']
snowflake_database = snowflake_auth['snowflake_database']
snowflake_schema = snowflake_auth['snowflake_schema']
snowflake_warehouse = snowflake_auth['snowflake_warehouse']
domo_dataset_id = domo_auth['domo_to_snowflake_mapping_dataset_id']
domo_client_id = domo_auth['domo_client_id']
domo_client_secret = domo_auth['domo_client_secret']

automate_domo_to_snowflake(snowflake_account, snowflake_username, snowflake_password, snowflake_database,
                          snowflake_schema, snowflake_warehouse,
                        domo_dataset_id, domo_client_id, domo_client_secret)



