from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import date
import time
import sqlalchemy as sa
from sqlalchemy import create_engine, Table, Column, Integer,Float, String, MetaData
import pandas as pd
import psycopg2 as psy

DAG_ID = "extract_populacao_cidade_ano"

path_populacao_cidade_ano = '/home/ubuntu/repository/airfloworchestrationtcc/extract_data/populacao_cidade_ano.csv'
filename_populacao_cidade_ano = 'populacao_cidade_ano'

def extract_data(path, filename):
    engine = create_engine('postgresql://db-teste.cvosgcqg050g.us-east-2.rds.amazonaws.com:5432/postgres?user=postgres&password=123456789')
    df = pd.read_csv(path,sep=";",header=0,encoding='UTF-8')
    metadata = MetaData() 
    def infer_sqlalchemy_type(dtype):
        """ Map pandas dtype to SQLAlchemy's types """
        if "int" in dtype.name:
            return Integer
        elif "float" in dtype.name:
            return Float
        elif "object" in dtype.name:
            return String(255)
        elif "date" in dtype.name:
            return date
        else:
            return String(255)      
    columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in df.dtypes.items()]
    tablename = 'extract_' + filename
    tablex = Table(tablename, metadata, *columns)
    with engine.connect() as conn:
        df.to_sql(name=tablename, con=conn.connection, index=False, chunksize=25000, method='None', if_exists='append')

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2024, 11, 3),
    schedule="@monthly",
    catchup=False,
    tags=["tcc"],
    ) as dag:

    extract_populacao = PythonOperator(
        task_id='extract_populacao',
        python_callable=extract_data,
        op_kwargs={
           "path": path_populacao_cidade_ano,
            "filename": filename_populacao_cidade_ano
        },
        dag=dag
    )

    extract_populacao
