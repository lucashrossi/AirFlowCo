from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

import requests
import psycopg2.extensions
from psycopg2.extras import execute_values


def dataapi():
    try:
        headers = {"Accept-Encoding": "gzip, deflate"}
        response = requests.get("https://api.luchtmeetnet.nl/open_api/measurements?station_number=&formula=&page=1&order_by=timestamp_measured&order_direction=desc",headers=headers)
        file_name = response.json()

        datax=file_name['data']
        df = pd.DataFrame.from_dict(datax)

        df.drop(df.index[50:1000], inplace=True)

        df.insert(0,'id','')
        df['id']=df.index+1

        df["timestamp_measured"] = pd.to_datetime(df["timestamp_measured"])
        df["station_number"] = df["station_number"].astype("string")
        df["formula"] = df["formula"].astype("string")


        url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
        port="5439"
        data_base="data-engineer-database"
        user="hgr79_coderhouse"
        pwd="kye79Z96x3Xe"

        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        cur = conn.cursor()
        table_name = 'airquality'
        # Define the columns you want to insert data into
        columns = ['id', 'station_number', 'value', 'timestamp_measured','formula']
        # Convert the DataFrame to a list of tuples
        values = [tuple(x) for x in df.to_numpy()]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        # Execute the INSERT statement using execute_values
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        # Close the cursor and connection
        cur.close()
        conn.close()


    except ValueError as e:
        print("Error the API")
        raise e
    


default_args={
    'owner': 'Hernan',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='DAGCoder',
    description= 'Nuestro primer dag usando python Operator',
    start_date=datetime(2023,5,11),
    schedule_interval='0 0 * * *'
    ) as dag:


    task1= PostgresOperator(
        task_id='crear_tabla_postgres',
        postgres_conn_id= 'Redshift',
        sql="""
            create table if not exists airquality(
                id INT PRIMARY KEY,
                station_number varchar(50),
                value float,
                timestamp_measured date,
                formula varchar(50)
            )
        """
    )
    task2 = PythonOperator(   
        task_id='dataapi',
        python_callable=dataapi,

    )
    task1 >> task2