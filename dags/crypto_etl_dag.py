from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Define the configuration of the DAG
default_args = {
    'owner': 'gabriel',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_data_dag',
    default_args=default_args,
    description='Get daily cryptocurrency information and update the database',
    schedule_interval=timedelta(days=1),
    catchup=True,
)

# Retrieve Redshift connection details using Airflow Hook
db_hook = BaseHook.get_hook(conn_id='amazon_redshift')
engine = create_engine(db_hook.get_uri(), execution_options={"autocommit": True})
# List of cryptocurrencies to obtain
coins = list(Variable.get("coins", deserialize_json=True).get("list"))

def drop_aux_table():
    query = "DROP TABLE IF EXISTS cryptodata_aux;"
    run_query(query)

def run_query(query):
    try:
        with engine.connect() as connection:
            connection.execute(query)
    except Exception as e:
        logging.error(f"Error executing the query: {e}")

def create_table_cryptodata_on_redshift():
    # Execute SQL command to create the table CryptoData
    create_sql = """
                CREATE TABLE if not exists CryptoData (
                    date date,
                    coin varchar(256),
                    opening float8,
                    closing float8,
                    lowest float8,
                    highest float8,
                    volume float8,
                    quantity float8,
                    amount int8,
                    avg_price float8,
                    primary key (date,coin)
                );   
            """
    run_query(create_sql) 

def get_conversion_real_to_peso(**kwargs):
    # Define the connection id
    connection_id = 'airflow_dw'

    # Instantiate a PostgresHook with the connection id
    postgres_hook = PostgresHook(postgres_conn_id=connection_id)

    start_date = kwargs['dag'].default_args['start_date']

    parsed_date = pd.to_datetime(start_date, format='%Y-%m-%d')

    # Define your SQL query
    sql_query = f"""
        SELECT date,rate
        FROM conversion_real_to_peso
        where date = '{parsed_date}'
    """

    # Fetch data from the database using the hook
    result = postgres_hook.get_pandas_df(sql_query)
    kwargs['ti'].xcom_push(key="dataframe_conversion_real_to_peso", value=result)
    # Now 'result' contains the data as a pandas DataFrame
    print(result)


def download_crypto_info(**kwargs):
    
    start_date = kwargs['dag'].default_args['start_date']
    print(f"Valor variable start_date {start_date}")
    df_uruguayan_data = kwargs['ti'].xcom_pull(task_ids='get_conversion_real_to_peso', key="dataframe_conversion_real_to_peso")
    rate = df_uruguayan_data['rate']
   

    # Get start date from the DAG configuration
    year =start_date.year
    month = start_date.month
    day = start_date.day

    # Drop auxiliary table
    drop_aux_table()

    # Download information for each cryptocurrency
    for coin in coins.keys():
        coin_data = obtain_daily_information(coin, year, month, day)
        if coin_data:
            df = create_dataframe(coin_data, coin, rate)
            print(f"Downloading information for {coin} on {year}/{month}/{day}:")
            print(df)
            # Store the DataFrame in the task instance
            kwargs['ti'].xcom_push(key=f"dataframe_{coin}", value=df)

def obtain_daily_information(coin, year, month, day):
    url = f'https://www.mercadobitcoin.net/api/{coin}/day-summary/{year}/{month}/{day}/'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to obtain information for {coin}. Status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"Request Exception: {e}")
        return None

def create_dataframe(data, coin, rate):
    if data:
        df = pd.DataFrame([data])
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['volume'] = pd.to_numeric(df['volume'])
        df['quantity'] = pd.to_numeric(df['quantity'])
        df['coin'] = coin
        df['opening'] = df['opening'] * rate
        df['closing'] = df['closing'] * rate
        df['lowest'] = df['lowest'] * rate
        df['highest'] = df['highest'] * rate
        df['avg_price'] = df['avg_price'] * rate
        return df
    return pd.DataFrame()

def load_data_to_redshift(**kwargs):
    ti = kwargs['ti']
    for coin in coins:
        df = ti.xcom_pull(task_ids='download_crypto_info', key=f"dataframe_{coin}")
        if not df.empty:
            df.to_sql("cryptodata_aux", engine, if_exists='append', index=False)
            print(f"Data loaded into Redshift for {coin}")

def execute_merge_on_redshift():
    # Execute SQL MERGE between the main table and the auxiliar table
    merge_sql = """
                MERGE INTO cryptodata
                USING cryptodata_aux AS source
                ON cryptodata.coin = source.coin AND cryptodata.date = source.date
                WHEN MATCHED THEN
                    UPDATE SET
                        opening = source.opening,
                        closing = source.closing,
                        lowest = source.lowest,
                        highest = source.highest,
                        volume = source.volume,
                        quantity = source.quantity,
                        amount = source.amount,
                        avg_price = source.avg_price
                WHEN NOT MATCHED THEN
                    INSERT (coin, date, opening, closing, lowest, highest, volume, quantity, amount, avg_price)
                    VALUES (source.coin, source.date, source.opening, source.closing, source.lowest, source.highest,
                            source.volume, source.quantity, source.amount, source.avg_price);             
            """
    run_query(merge_sql)        


def send_email():
    message = Mail(
                from_email=Variable.get("email_from"),
                to_emails=Variable.get("email_to"),
                subject="alerta",
                html_content="<strong>Body de la alerta</strong>")

    sg = SendGridAPIClient(Variable.get("sendgrid_api_key"))
    response = sg.send(message)
    print(response.status_code)

# Define tasks   
task_create_table_cryptodata_on_redshift = PythonOperator(
    task_id='create_table_cryptodata_on_redshift',
    python_callable=create_table_cryptodata_on_redshift,
    provide_context=True,
    dag=dag,
)

task_get_conversion_real_to_peso = PythonOperator(
    task_id='get_conversion_real_to_peso',
    python_callable=get_conversion_real_to_peso,
    provide_context=True,
    dag=dag,
)

task_download_crypto_info = PythonOperator(
    task_id='download_crypto_info',
    python_callable=download_crypto_info,
    provide_context=True,
    dag=dag,
)

task_load_data_to_redshift = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    provide_context=True,
    dag=dag,
)

task_execute_merge_on_redshift = PythonOperator(
    task_id='execute_merge_on_redshift',
    python_callable=execute_merge_on_redshift,
    provide_context=True,
    dag=dag,
)

task_send_email = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email,
    provide_context=True,
    dag=dag
)

# Set the task flow
task_create_table_cryptodata_on_redshift >> task_get_conversion_real_to_peso >>task_download_crypto_info >> task_load_data_to_redshift >> task_execute_merge_on_redshift >> task_send_email
