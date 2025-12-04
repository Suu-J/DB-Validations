'''
retrieve the number of tables from Oracle schema
retrieve the number of tables from Snowflake schema
display counts
'''

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

def count_oracle_tables(oracle_cursor, schema):
    oracle_cursor.execute(f"SELECT COUNT(*) FROM all_tables WHERE owner = '{schema}'")
    return oracle_cursor.fetchone()[0]

def count_snowflake_tables(snowflake_cursor):
    snowflake_cursor.execute("SHOW TABLES")
    return len(snowflake_cursor.fetchall())

try:
    with cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn) as oracle_connection:
        oracle_table_count = count_oracle_tables(oracle_connection.cursor(), oracle_schema)
        print("Number of tables in Oracle:", oracle_table_count)

        snowflake_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema
        )

        snowflake_cursor = snowflake_connection.cursor()

        snowflake_table_count = count_snowflake_tables(snowflake_cursor)
        print("Number of tables in Snowflake:", snowflake_table_count)

finally:
    snowflake_cursor.close()
    snowflake_connection.close()
