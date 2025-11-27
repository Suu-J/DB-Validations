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

# here create a validation table for a single table
def create_validation_table(snowflake_cursor, table_name):
    validation_table_name = f"{table_name}_VALIDATION"
    snowflake_cursor.execute(f"CREATE TABLE IF NOT EXISTS {snowflake_validation_schema}.{validation_table_name} ("
                             "ID VARCHAR(255),"
                             "STATUS BOOLEAN,"
                             "CREATED TIMESTAMP"
                             ")")

# we gotta validate all tables
def validate_all_tables(oracle_cursor, snowflake_cursor):
    # listing tables from OracleDB schema
    oracle_cursor.execute(f"SELECT table_name FROM all_tables WHERE owner = '{oracle_schema}'")
    oracle_tables = [row[0] for row in oracle_cursor.fetchall()]

    '''
    validating each table here,
    creating validation table if doesnt exists
    then fetch all rows from oDB table

    in for, we validate each row, and comapre each col in oDB row with corresponding col in sf row

    finally insert validation status into the validation table
    '''
    for table_name in oracle_tables:
        create_validation_table(snowflake_cursor, table_name)

        oracle_cursor.execute(f"SELECT * FROM _ARCHIVE.{table_name}")
        oracle_rows = oracle_cursor.fetchall()

        for oracle_row in oracle_rows:
            oracle_id = oracle_row[0]  # ID is the first column
            snowflake_cursor.execute(f"SELECT * FROM {table_name} WHERE ID = :1", (oracle_id,))
            snowflake_row = snowflake_cursor.fetchone()

            if snowflake_row:
                if oracle_row == snowflake_row:
                    status = True
                else:
                    status = False
            else:
                status = False

            created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            snowflake_cursor.execute(f"INSERT INTO {snowflake_validation_schema}.{table_name}_VALIDATION (ID, STATUS, CREATED) "
                                     "VALUES (:1, :2, :3)", (oracle_id, status, created))

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

        validate_all_tables(oracle_connection.cursor(), snowflake_cursor)

        snowflake_connection.commit()

finally:

    snowflake_cursor.close()
    snowflake_connection.close()