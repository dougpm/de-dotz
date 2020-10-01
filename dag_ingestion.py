"""
Essa DAG le uma lista de arquivos CSV do Storage, criando tabelas para cada um deles no BigQuery.
"""


import os
import datetime
import re

from airflow import configuration
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from dotz.utils import file_loader


DEFAULT_DAG_ARGS = {
    'owner': 'Dotz',
    'start_date': datetime.datetime(2020, 9, 29),
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
    'headers', "")

file_loader = file_loader.FileLoader()
headers = file_loader.load_files(HEADERS_DIR, '.txt')

# pasta criada dentro do bucket, contem os arquivos a serem lidos
raw_files_bucket = models.Variable.get("raw_files_bucket")

bq_dataset_landing = models.Variable.get("landing_dataset")

def storage_to_bq_task(filename):

    opt_dict = {
        'file_path': "{}/{}.csv".format(raw_files_bucket, filename),
        'header': getattr(headers, filename),
        'destination_table_id': "{}.{}".format(bq_dataset_landing, filename)
    }

    return DataFlowPythonOperator(
        task_id='load_{}'.format(filename),
        py_file=DATAFLOW_PIPELINE_FILE,
        job_name=re.sub(filename, '_', '-'),
        options=opt_dict)


with models.DAG(dag_id="dotz-ingestao",
                default_args=DEFAULT_DAG_ARGS,
                schedule_interval=None) as dag:

    
    filenames = 'bill_of_materials comp_boss price_quote'.split()
    csv_ingestions = []
    for file in filenames:
        csv_ingestions.append(storage_to_bq_task(file, headers))
    # bill_of_materials_opt = {
    #     'file_path': "{}/bill_of_materials.csv".format(raw_files_bucket),
    #     'header': headers.bill_of_materials,
    #     'destination_table_id': "{}.BILL_OF_MATERIALS".format(bq_dataset_landing)   
    # }

    # bill_of_materials = DataFlowPythonOperator(
    #     task_id='load_bill_of_materials',
    #     py_file=DATAFLOW_PIPELINE_FILE,
    #     job_name='bill-of-materials',
    #     options=bill_of_materials_opt)

    # comp_boss_opt = {
    #     'file_path': "{}/comp_boss.csv".format(raw_files_bucket),
    #     'header': headers.comp_boss,
    #     'destination_table_id': "{}.COMP_BOSS".format(bq_dataset_landing)   
    # }

    # comp_boss = DataFlowPythonOperator(
    #     task_id='load_comp_boss',
    #     py_file=DATAFLOW_PIPELINE_FILE,
    #     job_name='comp-boss',
    #     options=comp_boss_opt)

    # price_quote_opt = {
    #     'file_path': "{}/price_quote.csv".format(raw_files_bucket),
    #     'header': headers.price_quote,
    #     'destination_table_id': "{}.PRICE_QUOTE".format(bq_dataset_landing)   
    # }

    # price_quote = DataFlowPythonOperator(
    #     task_id='load_price_quote',
    #     py_file=DATAFLOW_PIPELINE_FILE,
    #     job_name='price-quote',
    #     options=price_quote_opt)

    # bq_task = BigQueryOperator(
    #     task_id="",
    #     use_legacy_sql=False,
    #     sql=,
    #     write_disposition='WRITE_TRUNCATE')

    # dummy = DummyOperator(task_id='')
