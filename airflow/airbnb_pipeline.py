from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import airflow

from airflow.models.connection import Connection
default_args = {
    'owner': 'Masoud',
    'depends_on_past': False,
    'email': ['masoud@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG('airbnb_price_prediction_demo', default_args=default_args)


build_project= BashOperator(
    task_id='build_project',
    bash_command='(cd {{ var.value.project_path }} && sbt package )',
    dag=dag,
)

ingest_data= SparkSubmitOperator(
    task_id='ingest_data',
    name='data ingestion',
    application='{{ var.value.project_file_path }}',
    java_class='task.main.DataIngestor',
    application_args=[
      "file:///{{ var.value.csv_path }}",
      "file:///{{ var.value.staging_path }}",
      "{{ var.value.table_name }}",
       "10214"
    ],
    jars='{{ var.value.extra_jars }}',
    dag=dag,
    conn_id='spark_default'
)

archive_csvs= BashOperator(
    task_id='archive_csvs',
    bash_command='mkdir -p {{ var.value.csv_path }}/../archive/;mkdir -p {{ var.value.csv_path }}/../archive/$(date +"%Y-%m-%d");mv {{ var.value.csv_path }}/*.csv {{ var.value.csv_path }}/../archive/$(date +"%Y-%m-%d")/;',
    dag=dag,
)


preprocessing_data=SparkSubmitOperator(
    task_id='preprocessing_data',
    name='data preprocessing',
    application='{{ var.value.project_file_path }}',
    java_class='task.main.ListingsTablePreprocessor',
    application_args=[
      "file:///{{ var.value.staging_path }}",
      "file:///{{ var.value.preproccessed_path }}",
      "{{ var.value.run_date }}"
    ],
    jars='{{ var.value.extra_jars }}',
    dag=dag,
    conn_id='spark_default'
)


model_data_preparation=SparkSubmitOperator(
    task_id='model_data_preparation',
    name='model data preparation',
    application='{{ var.value.project_file_path }}',
    java_class='task.main.ListingsModelDataPreparation',
    application_args=[
      "file:///{{ var.value.preproccessed_path }}",
      "file:///{{ var.value.model_data_path }}",
      "{{ var.value.run_date }}"
    ],
    jars='{{ var.value.extra_jars }}',
    dag=dag,
    conn_id='spark_default'
)

build_project >> ingest_data >> [archive_csvs,preprocessing_data]
preprocessing_data >> model_data_preparation

'''

model_data_preparation= BashOperator(
    task_id='model_data_preparation',
    bash_command='( '+project_path+' && bash run.sh 3 )',
    dag=dag,
)

build_project >> ingest_data
ingest_data >> preprocessing_data
preprocessing_data >> model_data_preparation
'''
