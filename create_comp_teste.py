# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "create_table_teste"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id="datamart"
        sql="""DROP TABLE IF EXISTS comp_test;""",
    )
   create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE comp_test AS select distinct ed.""Competência concessão"" from extract_datalake ed;
            """,
    )

    drop_table >> create_table 
