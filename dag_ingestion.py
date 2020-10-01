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

import utils


YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


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

EXTRA_FILES_DIR = os.path.join(
    configuration.get(
        'core',
        'dags_folder'),
    'dotz')

DATAFLOW_PIPELINE_FILE = os.path.join(
    EXTRA_FILES_DIR,
    'pipeline_dataflow.py')

HEADERS_DIR = os.path.join(
    EXTRA_FILES_DIR,
    'headers')

file_loader = utils.file_loader.FileLoader()
headers = file_loader.load_files(HEADERS_DIR)

# pasta criada dentro do bucket, contem os arquivos a serem lidos
#TODO: usar variavel de ambiente aqui
csv_files_path = "gs://de-dotz-2020/csvs"

with models.DAG(dag_id="dotz-ingestao",
                default_args=DEFAULT_DAG_ARGS,
                schedule_interval=None) as dag:

    bill_of_materials_opt = {
        'file_path': "{}/bill_of_materials.csv".format(csv_files_path),
        'header': headers.bill_of_materials,
        'destination_table_id': "{}.BILL_OF_MATERIALS".format(bq_dataset)   
    }

    bill_of_materials = DataFlowPythonOperator(
        task_id='process_bill_of_materials',
        py_file=DATAFLOW_PIPELINE_FILE,
        job_name='bill-of-materials',
        options=bill_of_materials_opt)

    # bq_task = BigQueryOperator(
    #     task_id="",
    #     use_legacy_sql=False,
    #     sql=,
    #     write_disposition='WRITE_TRUNCATE')

    # dummy = DummyOperator(task_id='')
