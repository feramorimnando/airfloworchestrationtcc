from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime
#ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "create_clean_cid_t1"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2024, 11, 3),
    schedule="@monthly",
    catchup=False,
    tags=["tcc"],
    ) as dag:
    drop_clean_cid_t1 = PostgresOperator(
        task_id = 'drop_clean_cid_t1',
        sql = """DROP TABLE IF EXISTS clean_cid_t1;""",
        postgres_conn_id = 'datamart',
        autocommit = True)
    create_clean_cid_t1 = PostgresOperator(
        task_id = 'create_clean_cid_t1',
        sql = """
            create table clean_cid_t1 as
            select distinct cd_cid, ds_cid from clean_datalake_t1 where cd_cid is not null;
            """
            ,
        postgres_conn_id = 'datamart',
        autocommit = True)

    drop_clean_cid_t1 >> create_clean_cid_t1
