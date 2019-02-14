import pandas as pd
from sqlalchemy import create_engine


def create_engine_pgsql(DB_CONN):
    engine = create_engine(DB_CONN)

    #'postgresql+psycopg2://postgres:postgres@localhost/postgres'
     #                      , echo=False)
    return engine


def insert_to_db(engine, df, table_name):
    df.to_sql(table_name, con=engine, if_exists="append")



