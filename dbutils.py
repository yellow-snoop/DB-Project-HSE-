import sqlalchemy
import psycopg2
import pandas as pd
import polars as pl

connection_string = 'postgresql://kujyshbv:sIM0DO3AXoak5VnzZoqe7W4uF64Ukx1m@cornelius.db.elephantsql.com/kujyshbv'

def connect(connection_string = connection_string) -> sqlalchemy.engine.base.Connection:
    print('Esteblishing connection...')
    engine = sqlalchemy.create_engine(connection_string)
    conn = engine.connect()
    print('Connected')
    return conn

def read(sql:str, conn:sqlalchemy.engine.base.Connection = None, pandas:bool = True) -> pd.DataFrame | pl.DataFrame:
    conn, conn_marker = connect(), 1 if conn == None else conn
    print('Executing query...')
    data = pl.read_database(sql, conn)
    if pandas: data = data.to_pandas()
    print('Data received')
    conn.close() if conn_marker == 1 else None
    return data