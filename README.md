# Coderhouse's Data Engineering Final Project
### Developed by Gabriel Murugosa

## Requirements
- Have Docker

## Description
This code gives you all the tools to run the specific DAG called `crypto_etl_dag.py`

What this DAG does is:

1. Pulls data from Mercadobitcoin(www.mercadobitcoin.net)
2. Get data from a Postgresql database with the rate conversion of Real currency to Peso.
3. Load the data into an Amazon Redshift table (cryptodata_aux) with the data for this day
4. Merge the information in another table in Amazon Redshift with all the historical information 

## Configuring your Amazon Redshift Connection
In the Airflow UI for your local Airflow environment, go to Admin > Connections. Click + to add a new connection, then select the connection type as Amazon Redshift.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/74af1ee0-d77a-4de4-a739-02bc3c9f34ad)

## Configuring your Postgresql Connection
In the Airflow UI for your local Airflow environment, go to Admin > Connections. Click + to add a new connection, then select the connection type as Postgresql

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/086b5ebd-33b7-4f8f-81e3-1e016b65cd55)


## Configuring your Airflow Variable with the list of coins
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

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/a2509c46-e3d4-4347-a4f8-c32007623f9a)

To send email alerts you need to create an account in the Sendgrid site https://sendgrid.com/en-us/pricing and get an API key.

After that you need to create three new variables in airflow sendgrid_api_key, sendgrid_email_from and sendgrid_email_to.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/f389bcb8-a2ae-404b-806f-b4a9bd792ddf)


## Usage
It's easy, just do:

1. `make build`
2. `make run`
3. `make get-admin-password` to get the password.
4. Enter `localhost:8080` in whatever browser you want.
5. Input `admin` as the user and the password you got on step 3. Without the `%` char.
6. Once inside, activate the DAG, wait for it to turn dark green and voila! The pipeline ran.
7. To kill everything, you can `make stop`

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/eee9ad75-2a60-461d-88f0-7577bcc12723)


https://github.com/gmurugosa/airflow_etl


## HELP!
Run `make help`.
