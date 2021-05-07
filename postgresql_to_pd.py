import psycopg2
import pandas as pd


def postgresql_to_pd(conn, select_query, column_names):
    cursor = conn.cursor()

    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("PSQL Error: %s" % error)
        cursor.close()
        return 1

    data = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(data, columns=column_names)
    return df
