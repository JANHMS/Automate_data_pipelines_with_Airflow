import configparser
import psycopg2
from drop_create_tables import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops the tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function recreates the tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This main function drop and recreate all the tables and uses the db 
    connections parameters from 
    the config file
    """

    print("Get config params") 
    config_file_path = 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_file_path))
    
    ENDPOINT               = config.get("CLUSTER","DWH_ENDPOINT")    
    DB_NAME                = config.get("CLUSTER","DWH_DB")
    DB_USER                = config.get("CLUSTER","DWH_DB_USER")
    DB_PASSWORD            = config.get("CLUSTER","DWH_DB_PASSWORD")
    DB_PORT                = config.get("CLUSTER","DWH_PORT")
    
    print("DB connection") 
    conn = psycopg2.connect("host={0} dbname={1} user={2} password={3} port={4}" \
                            .format(ENDPOINT,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT))
    cur = conn.cursor()

    print("drop tables") 
    drop_tables(cur, conn)
    print("create tables")     
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    