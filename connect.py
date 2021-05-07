import psycopg2
from get_config import get_config
import sys


def connect():
    conn = None
    try:
        params = get_config()

        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successfull!")
    return conn
