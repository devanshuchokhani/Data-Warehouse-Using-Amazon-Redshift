import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Iterate over all load queries that load the data from files in s3 bucket to the staging tables.

        * cur: cursor for database
        * conn: connection of the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Iterate over all insert queries that insert the data from staging table to final table.
        
        * cur: cursor for database
        * conn: connection of the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Read the db credentials from the config file, connects to the database, loads the s3 files into sage tables, loads the final tables from stage tables and close the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()