from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta
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
    'schedule_interval':'@weekly'
}

dag = DAG('airbnb_ml_pipeline', default_args=default_args)


build_project= BashOperator(
    task_id='build_project_and_test',
    bash_command='(cd {{ var.value.project_path }} && sbt package && sbt test)',
    dag=dag,
)

model_data_preparation= SparkSubmitOperator(
    task_id='train_model',
    name='modelDataPreparation',
    application='{{ var.value.project_file_path }}',
    java_class='task.main.MLListingsModel',
    application_args=[
      "file:///{{ var.value.model_data_path }}",
      "file:///{{ var.value.model_path }}",
      '{{ macros.ds_format(macros.ds_add(ds, -(var.value.days_before| int)), "%Y-%m-%d", "%Y-%m-%d") }}',
      "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%d') }}"
    ],
    jars='{{ var.value.extra_jars }}',
    dag=dag,
    conn_id='spark_default'
)

build_project >> model_data_preparation
