# Coderhouse's Data Engineering Final Project
### Developed by Gabriel Murugosa

## Requirements
- Have Docker

## Description
This code gives you all the tools to run the specific DAG called `crypto_etl_dag.py`

What this DAG does is:

1. Pulls data from Mercadobitcoin(www.mercadobitcoin.net)
2. Load the data into an Amazon Redshift table (cryptodata_aux) with the data for this day
3. Merge the information in another table in Amazon Redshift with all the historical information 

## Configuring your Amazon Redshift Connection
In the Airflow UI for your local Airflow environment, go to Admin > Connections. Click + to add a new connection, then select the connection type as Amazon Redshift.

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/74af1ee0-d77a-4de4-a739-02bc3c9f34ad)

## Configuring your Airflow Variable with the list of coins
To configure the information of the coins that you need to store on Amazon Redshift, it's necesary to create a variable with name coins.

In the Airflow UI for your local Airflow environment, go to Admin > Variables. Click + to add a new variable, then write the name and value in json format.
{"list":["BTC", "ETH", "USDT", "XRP", "SOL", "USDC", "ADA", "DOGE", "AVAX", "LTC"]}

![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/8e958a2d-470d-4105-b747-9503bdfa9117)

## Usage
It's easy, just do:

1. `make build`
2. `make run`
3. `make get-admin-password` to get the password.
4. Enter `localhost:8080` in whatever browser you want.
5. Input `admin` as the user and the password you got on step 3. Without the `%` char.
6. Once inside, activate the DAG, wait for it to turn dark green and voila! The pipeline ran.
7. To kill everything, you can `make stop`


[![image](https://github.com/gmurugosa/airflow_etl/assets/5313359/89879b66-3e51-4d47-b2ae-cbb2d83f1097)](https://github.com/gmurugosa/airflow_etl)https://github.com/gmurugosa/airflow_etl



## HELP!
Run `make help`.
