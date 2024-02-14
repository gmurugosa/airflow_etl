from datetime import datetime, timedelta
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

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

# Set up logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# Define configurable parameters
coins = Variable.get("coins", deserialize_json=True).get("list")
email_from = Variable.get("sendgrid_email_from")
email_to = Variable.get("sendgrid_email_to")
sendgrid_api_key = Variable.get("sendgrid_api_key")

# Define database connection
db_hook = 'amazon_redshift'
engine = create_engine(PostgresHook.get_connection(db_hook).get_uri(), execution_options={"autocommit": True})

# Task to drop auxiliary table if it exists
def drop_auxiliary_table():
    query = "DROP TABLE IF EXISTS cryptodata_aux;"
    execute_sql_query(query)

# Function to execute SQL query
def execute_sql_query(query):
    try:
        with engine.connect() as connection:
            connection.execute(query)
    except Exception as e:
        logging.error(f"Error executing the query: {e}")

# Task to create main table CryptoData on Redshift
def create_crypto_data_table():
    create_sql = """
        CREATE TABLE IF NOT EXISTS CryptoData (
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
            insert_datetime TIMESTAMP DEFAULT GETDATE(),
            update_datetime TIMESTAMP DEFAULT GETDATE(),
            primary key (date,coin)
        );   
    """
    execute_sql_query(create_sql) 

# Task to retrieve conversion rate of Real to Peso
def get_conversion_rate(**kwargs):
    try:
        connection_id = 'airflow_dw'
        postgres_hook = PostgresHook(postgres_conn_id=connection_id)
        start_date = kwargs['dag'].default_args['start_date'] - timedelta(days=1)
        parsed_date = pd.to_datetime(start_date, format='%Y-%m-%d')
        sql_query = f"""
            SELECT date, rate
            FROM conversion_real_to_peso
            WHERE date = '{parsed_date}'
        """
        result = postgres_hook.get_pandas_df(sql_query)
        kwargs['ti'].xcom_push(key="conversion_rate_data", value=result)
        logging.info("Conversion rate of Real to Peso retrieved successfully.")
    except Exception as e:
        logging.error(f"Error fetching conversion rate: {e}")

# Task to download cryptocurrency information
def download_crypto_information(**kwargs):
    try:
        start_date = kwargs['dag'].default_args['start_date'] - timedelta(days=1)
        conversion_rate_data = kwargs['ti'].xcom_pull(task_ids='get_conversion_rate', key="conversion_rate_data")
        rate = conversion_rate_data['rate']
        year = start_date.year
        month = start_date.month
        day = start_date.day
        drop_auxiliary_table()
        for coin in coins.keys():
            coin_data = obtain_daily_information(coin, year, month, day)
            if coin_data:
                df = create_dataframe(coin_data, coin, rate)
                kwargs['ti'].xcom_push(key=f"dataframe_{coin}", value=df)
                logging.info(f"Downloading information for {coin} on {year}/{month}/{day}:")
    except Exception as e:
        logging.error(f"Error downloading cryptocurrency information: {e}")

# Function to obtain cryptocurrency information from API
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

# Function to create DataFrame from obtained cryptocurrency data
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

# Task to load data into Redshift
def load_data_to_redshift(**kwargs):
    try:
        ti = kwargs['ti']
        for coin in coins:
            df = ti.xcom_pull(task_ids='download_crypto_information', key=f"dataframe_{coin}")
            if not df.empty:
                df.to_sql("cryptodata_aux", engine, if_exists='append', index=False)
                logging.info(f"Data loaded into Redshift for {coin}")
    except Exception as e:
        logging.error(f"Error loading data into Redshift: {e}")

# Task to execute merge operation on Redshift tables
def execute_merge_operation():
    try:
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
                    avg_price = source.avg_price,
                    update_datetime = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (coin, date, opening, closing, lowest, highest, volume, quantity, amount, avg_price,insert_datetime, update_datetime)
                VALUES (source.coin, source.date, source.opening, source.closing, source.lowest, source.highest,
                        source.volume, source.quantity, source.amount, source.avg_price, GETDATE(),GETDATE());             
        """
        execute_sql_query(merge_sql)
        logging.info("Merge operation executed successfully.")
    except Exception as e:
        logging.error(f"Error executing merge operation: {e}")

# Task to send email alert with coins above/below threshold volume
def send_email_alert_with_threshold(**kwargs):
    try:
        ti = kwargs['ti']
        coins_above_threshold = []
        coins_below_threshold = []
        threshold_message = ""  # Inicializar un string vacío para almacenar los valores del umbral
        
        for coin in coins:
            df = ti.xcom_pull(task_ids='download_crypto_information', key=f"dataframe_{coin}")
            if not df.empty:
                volume_min_threshold, volume_max_threshold = get_min_max_for_coin(coin)
                volume = df['volume'].iloc[0]
                threshold_message += f"<p>{coin} :<br/> - Minimum threshold: {volume_min_threshold}<br/> - Maximum threshold: {volume_max_threshold}<br/> - Current value: {volume}<br/>"
                if volume > volume_max_threshold:
                    coins_above_threshold.append(coin)
                    threshold_message += "- Status: Volume above threshold.<br/> "
                elif volume < volume_min_threshold:
                    coins_below_threshold.append(coin)
                    threshold_message += "- Status: Volume below threshold.<br/> "
        
        if coins_above_threshold or coins_below_threshold:
            execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dag_name = kwargs['dag'].dag_id

            # Construir el cuerpo del correo electrónico con formato HTML
            body = f"""
            <html>
            <body>
            <p>The DAG "{dag_name}" was executed on {execution_time}.</p>
            <p>Below is a summary of the coins that are below or above the volume treshold:</p>

            {threshold_message}

            <p>Best regards,<br/><br/>
            DAG Alerts Team</p>
            </body>
            </html>
            """

            # Creating email
            subject = f"Crypto Data DAG Alerts Volume Report on {execution_time}"

            mail = Mail(
                from_email=email_from,
                to_emails=email_to,
                subject=subject,
                html_content=body
            )

            # Sending email using SendGrid
            sg = SendGridAPIClient(sendgrid_api_key)
            response = sg.send(mail)
            logging.info(f"Mail sended. Status code: {response.status_code}")
        else:
            logging.info("There are no coins above or below the threshold. No email was sent.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")


# Function to extract min and max values of a coin
def get_min_max_for_coin(coin_name):
    coin_info = coins.get(coin_name)
    if coin_info:
        return coin_info.get('min_threshold_alert'), coin_info.get('max_threshold_alert')
    else:
        return None, None

# Define DAG tasks
task_create_crypto_data_table = PythonOperator(
    task_id='create_crypto_data_table',
    python_callable=create_crypto_data_table,
    dag=dag,
)

task_get_conversion_rate = PythonOperator(
    task_id='get_conversion_rate',
    python_callable=get_conversion_rate,
    provide_context=True,
    dag=dag,
)

task_download_crypto_information = PythonOperator(
    task_id='download_crypto_information',
    python_callable=download_crypto_information,
    provide_context=True,
    dag=dag,
)

task_load_data_to_redshift = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    provide_context=True,
    dag=dag,
)

task_execute_merge_operation = PythonOperator(
    task_id='execute_merge_operation',
    python_callable=execute_merge_operation,
    dag=dag,
)

task_send_email_alert = PythonOperator(
    task_id='send_email_alert',
    python_callable=send_email_alert_with_threshold,
    dag=dag,
)

# Define DAG task dependencies
task_create_crypto_data_table >> task_get_conversion_rate >> task_download_crypto_information >> task_load_data_to_redshift >> task_execute_merge_operation >> task_send_email_alert
