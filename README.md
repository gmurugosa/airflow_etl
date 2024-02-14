# Coderhouse's Data Engineering Final Project
### Developed by Gabriel Murugosa

## Requirements
- Docker

## Description
This project implements an Airflow Directed Acyclic Graph (DAG) named crypto_data_dag to fetch daily cryptocurrency information from an API.
The information is in Real (Brazilian currency) so the process converts it to the Uruguayan Peso using data in a PostgreSQL database.
After this transformation load the data in Amazon Redshift and it sends email alerts with summaries of coins volume exceeding predefined thresholds.

What this DAG does is:

1. Create Crypto Data Table: Creates a table named CryptoData in the PostgreSQL database if it doesn't exist.
2. Get Conversion Rate: Fetches the conversion rate of Real to Peso from the database.
3. Download Crypto Information: Downloads daily cryptocurrency information from an API, processes it, and stores it as a DataFrame.
4. Load the data into an Amazon Redshift table (cryptodata_aux) with the data for this day
5. Merge the information in another table in Amazon Redshift with all the historical information
6. Send an email with a summary of the coin volume exceeding predefined thresholds.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/eee9ad75-2a60-461d-88f0-7577bcc12723)

## Configuring your Amazon Redshift Connection
In the Airflow UI for your local Airflow environment, go to Admin > Connections. Click + to add a new connection, then select the connection type as Amazon Redshift.

Create the connection using the image below as a reference.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/74af1ee0-d77a-4de4-a739-02bc3c9f34ad)

## Configuring your Postgresql Connection
In the Airflow UI for your local Airflow environment, go to Admin > Connections. Click + to add a new connection, then select the connection type as Postgresql

Create the connection using the image below as a reference.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/1048216e-d0f3-416b-b2b5-6c934ad67d36)



## Configuring your Airflow Variables
To configure the information of the coins that you need to store on Amazon Redshift, it's necessary to create a variable with name coins.

In the Airflow UI for your local Airflow environment, go to Admin > Variables. Click + to add a new variable, then write the name and value in JSON format.

```yaml

{
   "list":{
      "BTC":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "ETH":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "USDT":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "XRP":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "SOL":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "USDC":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "USDT":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "ADA":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "DOGE":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "AVAX":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "LTC":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      },
      "ADA":{
         "min_threshold_alert":240,
         "max_threshold_alert":290
      }
   }
}
```

Create the variables using the image below as a reference.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/a2509c46-e3d4-4347-a4f8-c32007623f9a)

To send email alerts you need to create an account in the Sendgrid site https://sendgrid.com/en-us/pricing and get an API key.

After that you need to create three new variables in airflow: 

1. sendgrid_api_key: API key for SendGrid to send email alerts.
2. sendgrid_email_from: Email address from which alerts will be sent.
3. sendgrid_email_to: Email address(es) to which alerts will be sent.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/f389bcb8-a2ae-404b-806f-b4a9bd792ddf)

## Email Example:

Subject: 

Crypto Data DAG Alerts Volume Report on 2024-02-14 19:53:52

Body:

The DAG "crypto_data_dag" was executed on 2024-02-14 19:53:52.

Below is a summary of the coins that are below or above the volume treshold:

BTC :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 12495613.74042248
- Status: Volume above threshold.
  
ETH :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 4327599.94612477
- Status: Volume above threshold.
  
USDT :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 282713.56123292
- Status: Volume above threshold.
  
XRP :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 260561.22479138
- Status: Volume above threshold.
  
SOL :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 1234919.11064081
- Status: Volume above threshold.
  
USDC :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 392499.79941562
- Status: Volume above threshold.
  
ADA :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 314597.16245071
- Status: Volume above threshold.
  
DOGE :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 145602.25295119
- Status: Volume above threshold.
  
AVAX :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 197800.99605916
- Status: Volume above threshold.
  
LTC :
- Minimum threshold: 240
- Maximum threshold: 290
- Current value: 91558.20391983
- Status: Volume above threshold.
  
Best regards,

DAG Alerts Team

## Usage
It's easy, just do:

1. `make build`
2. `make run`
3. `make get-admin-password` to get the password.
4. Enter `localhost:8080` in whatever browser you want.
5. Input `admin` as the user and the password you got on step 3. Without the `%` char.
6. Once inside, activate the DAG, wait for it to turn dark green and voila! The pipeline ran.
7. To kill everything, you can `make stop`

## HELP!
Run `make help`.
