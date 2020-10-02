"""
Essa DAG le uma lista de arquivos CSV do Storage, criando tabelas para cada um deles no BigQuery.
"""


import os
import datetime
import re
import logging
import ntpath

from airflow import configuration
from airflow import models
from google.cloud import storage
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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

bq_dataset_landing = models.Variable.get("landing_dataset")
gcs_bucket = models.Variable.get("gcs_bucket")

csv_files = [
    'bill_of_materials',
    'price_quote',
    'comp_boss'
]

csvs_folder = "csvs"

raw_files_path = os.path.join(gcs_bucket, csvs_folder)
#tags utilizadas para mover os csvs para diretorios especificos depois de serem processados, com sucesso ou nao
successful_tag = 'processed'
failed_tag = 'failed'

def storage_to_bq_task(filename):

    opt_dict = {
        'file_path': "{}/{}.csv".format(raw_files_path, filename),
        'header': getattr(headers, filename),
        'destination_table_id': "{}.{}".format(bq_dataset_landing, filename)
    }

    return DataFlowPythonOperator(
        task_id='load_{}'.format(filename),
        py_file=DATAFLOW_PIPELINE_FILE,
        job_name=re.sub('_', '-', filename),
        options=opt_dict)

def move_to_completion_bucket(bucket_name, origin_folder, status_tag, csv_files, **kwargs):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(ntpath.basename(bucket_name))
    
    for file in csv_files:
        source_object = file + ".csv"
        file_path = "{}/{}".format(origin_folder,source_object)
        file_blob = bucket.get_blob(file_path)
        target_object = os.path.join(status_tag, source_object)
        
        logging.info('Moving {} to {}'.format(
            os.path.join(source_object),
            os.path.join(target_object)))

        bucket.copy_blob(file_blob, bucket, target_object)

        logging.info('Deleting {}'.format(os.path.join(source_object)))

        bucket.delete_blob(file_path)
                    

with models.DAG(dag_id="dotz-ingestao",
                default_args=DEFAULT_DAG_ARGS,
                schedule_interval=None) as dag:

    csv_ingestion_tasks = []
    for file in csv_files:
        csv_ingestion_tasks.append(storage_to_bq_task(file))

    success_move_task = PythonOperator(
        task_id='move_to_success_folder',
        python_callable=move_to_completion_bucket,
        op_args= [gcs_bucket, csvs_folder, successful_tag, csv_files],
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS)

    failure_move_task = PythonOperator(
        task_id='move_to_failure_folder',
        python_callable=move_to_completion_bucket,
        op_args=[gcs_bucket, csvs_folder, failed_tag, csv_files],
        provide_context=True,
        trigger_rule=TriggerRule.ALL_FAILED)

    for task in csv_ingestion_tasks:
        task >> success_move_task
        task >> failure_move_task

    # bq_task = BigQueryOperator(
    #     task_id="",
    #     use_legacy_sql=False,
    #     sql=,
    #     write_disposition='WRITE_TRUNCATE')

    # dummy = DummyOperator(task_id='')
