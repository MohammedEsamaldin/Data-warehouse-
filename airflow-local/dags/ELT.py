from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os
dag_path = os.getcwd()
def load_data(): 
    conn_string = 'postgres-localhost'
    db = create_engine(conn_string)
    conn = db.connect()
    records = pd.read_csv(f"{dag_path}data/20181024_d1_0830_0900.csv")
    records.to_sql('data1', conn, if_exists='replace', index=False)
  
with DAG("my_dag", # Dag id
start_date=datetime(2021, 1 ,1), # start date, the 1st of January 2021 
schedule_interval='@daily',  # Cron expression, here it is a preset of Airflow, @daily means once every day.
catchup=False  # Catchup 
) as dag:
        create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres-localhost",
        sql="""
                CREATE TABLE IF NOT EXISTS booking_record (
                    
    "id" SERIAL NOT NULL,
    "track_id" TEXT NOT NULL,
    "lat" TEXT NOT NULL,
    "lon" TEXT DEFAULT NULL,
    "speed" TEXT DEFAULT NULL,
    "lon_acc" TEXT DEFAULT NULL,
    "lat_acc" TEXT DEFAULT NULL,
    "time" TEXT DEFAULT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT fk_trajectory
        FOREIGN KEY("track_id") 
            REFERENCES trajectories(track_id)
            ON DELETE CASCADE
                );
          """,
    )
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data
)
create_table >> load_data