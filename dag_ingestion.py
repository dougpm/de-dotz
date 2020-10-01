"""
A DAG le uma lista de arquivos CSV do Storage, criando tabelas para cada um deles no BigQuery.
"""


import os
import datetime

from airflow import configuration
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.dummy_operator import DummyOperator


YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


gcs_bucket_name = "gs://de-dotz-2020"
# pasta criada dentro do bucket, contem os arquivos a serem lidos
csv_files_path = "{}/csvs".format(gcs_bucket_name)
# dataset que vai receber as tabelas criadas a partir dos csvs
bq_dataset = "landing"
DEFAULT_DAG_ARGS = {
    'owner': 'Dotz',
    'start_date': YESTERDAY,
    'email': 'douglaspmartins0@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'project_id': models.Variable.get('project_id'),
    'dataflow_default_options': {
        'region': 'us-east1',
        'max_num_workers': '20',
        'machine_type': 'n1-standard-1',
        'disk_size_gb': '50',
        'project': models.Variable.get('project_id'),
        'temp_location': models.Variable.get('staging_bucket'),
        'runner': 'DataflowRunner',
        # 'service_account_email': 'douglas@tactile-sweep-291117.iam.gserviceaccount.com'
    }
}

EXTRA_FILES = os.path.join(
    configuration.get(
        'core',
        'dags_folder'),
    'dotz')

CSV_TO_BQ_PIPELINE = os.path.join(
    EXTRA_FILES,
    'pipeline_dataflow.py')

with models.DAG(dag_id="dotz-ingestao",
                default_args=DEFAULT_DAG_ARGS,
                schedule_interval=None) as dag:

    bill_of_materials_opt = {
        'file_path': "{}/bill_of_materials.csv".format(csv_files_path),
        'fields': "tube_assembly_id,component_id_1,quantity_1,component_id_2,quantity_2,component_id_3,quantity_3,component_id_4,quantity_4,component_id_5,quantity_5,component_id_6,quantity_6,component_id_7,quantity_7,component_id_8,quantity_8",
        'destination_table_id': "{}.BILL_OF_MATERIALS".format(bq_dataset)   
    }

    bill_of_materials = DataFlowPythonOperator(
        task_id='process_bill_of_materials',
        py_file=CSV_TO_BQ_PIPELINE,
        job_name='bill-of-materials',
        options=bill_of_materials_opt)

    # bq_task = BigQueryOperator(
    #     task_id="",
    #     use_legacy_sql=False,
    #     sql=,
    #     write_disposition='WRITE_TRUNCATE')

    # dummy = DummyOperator(task_id='')
