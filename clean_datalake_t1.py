from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime
#ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "create_clean_datalake_t1"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2024, 11, 3),
    schedule="@monthly",
    catchup=False,
    tags=["tcc"],
    ) as dag:
    drop_clean_datalake_t1 = PostgresOperator(
        task_id = 'drop_clean_datalake_t1',
        sql = """DROP TABLE IF EXISTS clean_datalake_t1;""",
        postgres_conn_id = 'datamart',
        autocommit = True)
    create_clean_datalake_t1 = PostgresOperator(
        task_id = 'create_clean_datalake_t1',
        sql = """
            create table clean_datalake_t1 as
            select 
            date(
            concat(
                split_part("Competência concessão",'/',2),
                case 
                    when split_part("Competência concessão",'/',1) = 'janeiro'   then '-01-01'
                    when split_part("Competência concessão",'/',1) = 'fevereiro' then '-02-01'
                    when split_part("Competência concessão",'/',1) = 'março'     then '-03-01'
                    when split_part("Competência concessão",'/',1) = 'abril'     then '-04-01'
                    when split_part("Competência concessão",'/',1) = 'maio'      then '-05-01'
                    when split_part("Competência concessão",'/',1) = 'junho'     then '-06-01'
                    when split_part("Competência concessão",'/',1) = 'julho'     then '-07-01'
                    when split_part("Competência concessão",'/',1) = 'agosto'    then '-08-01'
                    when split_part("Competência concessão",'/',1) = 'setembro'  then '-09-01'
                    when split_part("Competência concessão",'/',1) = 'outubro'   then '-10-01'
                    when split_part("Competência concessão",'/',1) = 'novembro'  then '-11-01'
                    when split_part("Competência concessão",'/',1) = 'dezembro'  then '-12-01'
                end)
            ) mesref,
            regexp_substr("Mun Resid",'([0-9]*)') as cd_municipio,
            regexp_substr("Mun Resid",'([A-Z][A-Z])') as UF_municipio,
            upper(retira_acentuacao(regexp_replace("Mun Resid",'(([0-9]*)\-)([A-Z][A-Z]\-)',''))) as ds_municipio,
            "Espécie" ds_especie,
            regexp_substr("CID.1",'[A-Z]*[0-9]*.[0-9]') cd_CID,
            trim(regexp_substr("CID.1",'\s+.*')) ds_CID,
            "Despacho",
            case when "Dt Nascimento" = '00/00/0000' then to_date('01/01/1900','DD/MM/YYYY') when "Dt Nascimento" = '{ñ class}' then to_date('01/01/1900','DD/MM/YYYY') else to_date("Dt Nascimento",'DD/MM/YYYY') end as data_nascimento,
            "Sexo." as sexo,
            "Clientela",
            "Vínculo dependentes" as vinculo_dependentes,
            "Forma Filiação"  as forma_filiacao,
            cast(replace("Qt SM RMI",',','.') as float) as qt_sm_rmi,
            "Ramo Atividade" as ramo_atividade,
            case when "Dt DCB" = '00/00/0000' then to_date('01/01/1900','DD/MM/YYYY') when "Dt DCB" = '{ñ class}' then to_date('01/01/1900','DD/MM/YYYY') else to_date("Dt DCB",'DD/MM/YYYY') end as data_DCB,
            case when "Dt DDB" = '00/00/0000' then to_date('01/01/1900','DD/MM/YYYY') when "Dt DDB" = '{ñ class}' then to_date('01/01/1900','DD/MM/YYYY') else to_date("Dt DDB",'DD/MM/YYYY') end as data_DDB,
            case when "Dt DIB" = '00/00/0000' then to_date('01/01/1900','DD/MM/YYYY') when "Dt DIB" = '{ñ class}' then to_date('01/01/1900','DD/MM/YYYY') else to_date("Dt DIB",'DD/MM/YYYY') end as data_DIB,
            "Acordo Internacional" as acordo_internacional,
            "Classificador PA" as classificador_pa
            from extract_datalake;
            """
            ,
        postgres_conn_id = 'datamart',
        autocommit = True)

    trg_clean_cid_t1 = TriggerDagRunOperator(
        task_id="trigger_create_clean_cid_t1",
        trigger_dag_id="create_clean_cid_t1",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )
    trg_clean_municipio_t1 = TriggerDagRunOperator(
        task_id="trigger_create_clean_cid_t1",
        trigger_dag_id="create_clean_municipio_t1",
        wait_for_completion=True,
        deferrable=True,  # Note that this parameter only exists in Airflow 2.6+
    )

    drop_clean_datalake_t1 >> create_clean_datalake_t1 >> [trg_clean_cid_t1,trg_clean_municipio_t1]
