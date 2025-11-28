'''
we are passing table name here, validating one table only

checking equality through the whole row, getting rows from oracledb row,
get all rows from oracledb, validate each row, get ids from oracledb row,
checking if rows exists in snowflake as well

'''

import os
from datetime import datetime
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
oracle_schema = ""

snowflake_user = ""
snowflake_password = ""
snowflake_account = ""
snowflake_database = ""
snowflake_schema = ""
snowflake_validation_schema = ""  # validation table schema 



def validate_table(oracle_cursor, snowflake_cursor, table_name):
    oracle_cursor.execute(f"SELECT * FROM SFCC_ARCHIVE.{table_name}")
    oracle_rows = oracle_cursor.fetchall()

    for oracle_row in oracle_rows:
        oracle_id = oracle_row[0]

        snowflake_cursor.execute(f"SELECT * FROM SFCC_ARCHIVE.{table_name} WHERE ID = %s", (oracle_id,))
        snowflake_row = snowflake_cursor.fetchone()

        if snowflake_row:
            if oracle_row == snowflake_row:
                status = True
            else:
                status = False
        else:
            status = False

        created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        snowflake_cursor.execute(f"INSERT INTO {snowflake_validation_schema}.{table_name}_VAL (SID, STATUS, CREATED) "
                                 "VALUES (%s, %s, %s)", (oracle_id, status, created))

try:
    with cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn) as oracle_connection:
        snowflake_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema
        )

        snowflake_cursor = snowflake_connection.cursor()

        table_name = "USER__C"

        validate_table(oracle_connection.cursor(), snowflake_cursor, table_name)

        snowflake_connection.commit()

finally:
    snowflake_cursor.close()
    snowflake_connection.close()


