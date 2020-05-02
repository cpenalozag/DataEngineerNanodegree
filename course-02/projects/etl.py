import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    Executes the queries that load the S3 data into the staging tables.
  
    Parameters: 
    cur (psycopg2 cursor): Database cursor
    conn (psycopg2 connection): Connection instance
  
    Returns: 
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ 
    Insert data to the dimensional model tables from the staging tables.
  
    Parameters: 
    cur (psycopg2 cursor): Database cursor
    conn (psycopg2 connection): Connection instance
  
    Returns: 
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    Main method for the ETL process.
    
    Configures the connection to the Redshift cluster and runs the required functions.
  
    Parameters: 
    None
  
    Returns: 
    None
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