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


def oracle_table_exists(oracle_cursor, table_name, schema):
    oracle_cursor.execute(f"SELECT COUNT(*) FROM all_tables WHERE table_name = '{table_name}' AND owner = '{schema}'")
    return oracle_cursor.fetchone()[0] > 0

def snowflake_table_exists(snowflake_cursor, table_name):
    snowflake_cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return snowflake_cursor.rowcount > 0

try:
    with cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn) as oracle_connection:
        if oracle_table_exists(oracle_connection.cursor(), "<TABLE_NAME>", oracle_schema):
            print("<TABLE_NAME> exists in Oracle.")
        else:
            print("<TABLE_NAME> does not exist in Oracle.")

        snowflake_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema
        )

        snowflake_cursor = snowflake_connection.cursor()

        if snowflake_table_exists(snowflake_cursor, "<TABLE_NAME>"):
            print("<TABLE_NAME> exists in Snowflake.")
        else:
            print("<TABLE_NAME> does not exist in Snowflake.")

finally:
    # Close the Snowflake cursor and connection
    snowflake_cursor.close()
    snowflake_connection.close()