import configparser
import psycopg2
from sql_queries import *


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for i, query in enumerate(create_table_queries):
        cur.execute(query)
        print("--- Creating %s table ---" % (create_drop_names[i]))
        conn.commit()
    print("--- Done ---")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("{}://{}:{}@{}:{}/{}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()