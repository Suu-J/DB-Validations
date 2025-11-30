# validating row counts here

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
oracle_schema = ""

snowflake_user = ""
snowflake_password = ""
snowflake_account = ""
snowflake_database = ""
snowflake_schema = ""
snowflake_validation_schema = ""  # validation table schema 

def get_oracle_table_row_count(oracle_cursor, table_name):
    oracle_cursor.execute(f"SELECT COUNT(*) FROM SFCC_ARCHIVE.{table_name}")
    return oracle_cursor.fetchone()[0]

def get_snowflake_table_row_count(snowflake_cursor, table_name):
    snowflake_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return snowflake_cursor.fetchone()[0]


try:
    with cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn) as oracle_connection:
        oracle_cursor = oracle_connection.cursor()
        oracle_cursor.execute(f"SELECT table_name FROM all_tables WHERE owner = '{oracle_schema}'")
        oracle_table_names = [row[0] for row in oracle_cursor.fetchall()]

        total_row_count_oracle = 0
        oracle_row_counts = {}
        print("Tables in OracleDB:")
        for table_name in oracle_table_names:
            row_count_oracle = get_oracle_table_row_count(oracle_cursor, table_name)
            oracle_row_counts[table_name] = row_count_oracle
            print(f"{table_name}: {row_count_oracle} rows")
            total_row_count_oracle+=row_count_oracle
        
        oracle_cursor.close()

    snowflake_connection = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema
    )

    snowflake_cursor = snowflake_connection.cursor()

    snowflake_cursor.execute("SHOW TABLES")
    snowflake_table_names = [row[1] for row in snowflake_cursor]

    print("\nTables in Snowflake:")
    for table_name in snowflake_table_names:
        row_count_snowflake = get_snowflake_table_row_count(snowflake_cursor, table_name)
        print(f"{table_name}: {row_count_snowflake} rows")

    print("\nComparison of row counts between OracleDB and Snowflake:")
    unmatched = 0
    for table_name, row_count_oracle in oracle_row_counts.items():
        row_count_snowflake = get_snowflake_table_row_count(snowflake_cursor, table_name)
        if row_count_oracle == row_count_snowflake:
            print(f"{table_name}: Row counts match")
        else:
            unmatched+=1
            print(f"{table_name}: Row counts do not match (Oracle: {row_count_oracle}, Snowflake: {row_count_snowflake})")

    print("\n\nTOTAL UNMATCHED ROWS = ", unmatched)
    print("\n\nTOTAL ROW COUNT = ", total_row_count_oracle)



finally:
    snowflake_cursor.close()
    snowflake_connection.close()