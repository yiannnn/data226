# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def return_last_90d_price(symbol):
  vantage_api_key = Variable.get('alpha_password')
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
  r = requests.get(url)
  data = r.json()
  results = []
  for d in data["Time Series (Daily)"]: # here d is a date: "YYYY-MM-DD"
    stock_info = data["Time Series (Daily)"][d] 
    stock_info["date"] = d 
    results.append(stock_info)
  return results

@task
def load_v2(con, records):
  target_table = "dev.raw_data.stock_price" 
  try:
    con.execute("BEGIN;")
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      date NUMBER(6,0) PRIMARY KEY, open NUMBER(38,0),
      high NUMBER(38,0),
      low NUMBER(38,0),
      close NUMBER(38,0),
      volume NUMBER(38,0) 
    )""")
    for r in records:
      open = r["1. open"]
      high = r["2. high"]
      low = r["3. low"]
      close = r["4. close"]
      volume = r["5. volume"]
      date = r["date"]
      insert_sql = f"INSERT INTO {target_table} (date, open, high, low, close, volume) VALUES ({date}, {open}, {high}, {low}, {close}, {volume})"
      con.execute(insert_sql) 
    con.execute("COMMIT;")
  except Exception as e: 
    con.execute("ROLLBACK;") 
    print(e)
    raise e


with DAG(
    dag_id = 'stock_v2',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    #target_table = "dev.raw_data.country_capital"
    #url = Variable.get("country_capital_url")
    cur = return_snowflake_conn()
    symbol = Variable.get("stock_symbol")
    price_list = return_last_90d_price(symbol)
    #create_table(cur)

    # data = extract(url)
    # lines = transform(data)
    load_v2(cur, price_list)