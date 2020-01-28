from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import airflow

default_args = {
    'owner': 'Masoud',
    'depends_on_past': False,
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(2),
    'schedule_interval':'@daily'
}

dag = DAG('airbnb_data_preparation_demo', default_args=default_args)


build_project= BashOperator(
    task_id='build_project_and_test',
    bash_command='(cd {{ var.value.project_path }} && sbt package && sbt test)',
    dag=dag,
)

get_row_numbers= BashOperator(
    task_id='get_row_numbers',
    bash_command='echo $(csvtool height {{ var.value.csv_path }}/{{ var.value.table_name }}*.csv)',
    xcom_push=True,
    dag=dag,
)

get_files_numbers= BashOperator(
    task_id='get_files_numbers',
    bash_command= 'echo $(find {{ var.value.csv_path }}/ -type f | wc -l)',
    xcom_push=True,
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
      "{{ ti.xcom_pull('get_row_numbers') | int - ti.xcom_pull('get_files_numbers') | int}}"
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

preprocessing_data= SparkSubmitOperator(
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

model_data_preparation= SparkSubmitOperator(
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

build_project >> [get_row_numbers,get_files_numbers] >> ingest_data >> [archive_csvs,preprocessing_data]
preprocessing_data >> model_data_preparation
