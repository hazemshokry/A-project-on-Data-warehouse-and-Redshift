import configparser
import psycopg2
import time
from sql_queries import *


def load_staging_tables(cur, conn):
    for i, query in enumerate(copy_table_queries):
        print("--- Loading %s table ---" % copy_table_names[i])
        start_time = time.time()
        cur.execute(query)
        conn.commit()
        print("--- Done in %s seconds ---" % round((time.time() - start_time), 2))
        cur.execute(staging_tables_row_count[i])
        print("Number of rows affected: %s" % cur.fetchone())
        conn.commit()


def insert_tables(cur, conn):
    for i, query in enumerate(insert_table_queries):
        print("--- Inserting Data into %s table ---" % insert_table_names[i])
        start_time = time.time()
        cur.execute(query)
        conn.commit()
        print("--- Done in %s seconds ---" % round((time.time() - start_time), 2))
        cur.execute(analytics_tables_row_count[i])
        print("Number of rows affected: %s" % cur.fetchone())
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("{}://{}:{}@{}:{}/{}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()