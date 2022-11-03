import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)
  
  
conn_string = 'postgres://user:password@host/data1'
  
db = create_engine(conn_string)
conn = db.connect()
  
def load_to_sql(self, table):
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    table.to_sql(f'stg.{table}', con=conn, if_exists='replace', index=False)

