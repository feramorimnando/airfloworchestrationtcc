from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime
#ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "create_table_teste"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@monthly",
    catchup=False,
    ) as dag:
    drop_table = PostgresOperator(
        task_id = 'drop_table',
        sql = """DROP TABLE IF EXISTS comp_teste;""",
        postgres_conn_id = 'datamart',
        autocommit = True)
    create_table = PostgresOperator(
        task_id = 'create_table',
        sql = """
            CREATE TABLE comp_test AS select distinct ed.""Competência concessão"" from extract_datalake ed;
            """
            ,
        postgres_conn_id = 'datamart',
        autocommit = True)

    drop_table >> create_table 
