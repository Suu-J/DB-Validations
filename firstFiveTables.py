# only displaying first five in each

import os

os.environ["ORACLE_BASE"] = "/opt/oracle"
os.environ["ORACLE_HOME"] = "/opt/oracle/product/19.3.0/client_1"
os.environ["PATH"] = os.environ["ORACLE_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["SHLIB_PATH"] = os.environ["ORACLE_HOME"] + "/lib:" + os.environ["ORACLE_HOME"] + "/bin"
os.environ["LD_LIBRARY_PATH"] = os.environ["ORACLE_HOME"] + "/lib:/usr/lib"


import cx_Oracle
import snowflake.connector

oracle_user = ""
oracle_password = ""
oracle_dsn = ""
oracle_schema= ""

snowflake_user = ""
snowflake_password = ""
snowflake_account = ""
snowflake_database = ""
snowflake_schema = ""
snowflake_validation_schema = ""  # validation table schema 

def get_oracle_table_names(oracle_cursor, schema):
    oracle_cursor.execute(f"SELECT table_name FROM all_tables WHERE owner = '{schema}'")
    return [row[0] for row in oracle_cursor.fetchall()]

def get_snowflake_table_names(snowflake_cursor):
    snowflake_cursor.execute("SHOW TABLES")
    return [row[1] for row in snowflake_cursor]

try:
    with cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn) as oracle_connection:
        oracle_table_names = get_oracle_table_names(oracle_connection.cursor(), oracle_schema)

        print("First five tables from Oracle:")
        for table_name in oracle_table_names[:5]:
            print(table_name)

        snowflake_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema
        )

        snowflake_cursor = snowflake_connection.cursor()

        snowflake_table_names = get_snowflake_table_names(snowflake_cursor)

        print("\nFirst five tables from Snowflake:")
        for table_name in snowflake_table_names[:5]:
            print(table_name)

finally:
    snowflake_cursor.close()
    snowflake_connection.close()