from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.time_delta import TimeDeltaSensor

# GCS and BigQuery operators (import them)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,
    BigQueryDeleteTableOperator
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSListObjectsOperator,
    GCSDeleteBucketOperator
)

email=['venkatesh@gmail.come',]
proj_id='venkateshgcpdata'
task_id=100
airflowgcsbucket='airflowbucket123456'
Table1='gcsairtable2'
bq_dataset='mydataset'

default_args={
                'start_date':days_ago(0),
                'depends_on_past':False,
                'email':email,
                'email_on_failure':True,
                'email_on_retry':False,
                'retries':3,
                'retry_delay':timedelta(minutes=5)
                }
dag=DAG(
         'operators_testing',
         default_args=default_args,
         description='i am testing oparetors',
         schedule_interval='*/10 * * * *',
         max_active_runs=3,
         catchup=False,
         dagrun_timeout=timedelta(minutes=5)
         )
dummy_operator=DummyOperator(
                              task_id='dag_started',
                              dag=dag
                              )         
create_bucket=GCSCreateBucketOperator(
                                       task_id='create_bucket',
                                       bucket_name=airflowgcsbucket,
                                       dag=dag
                                       )
list_objects=GCSListObjectsOperator(
                                     task_id='list_objects',
                                     bucket=airflowgcsbucket,
                                     dag=dag
                                     )
create_table = BigQueryCreateTableOperator(
    task_id="create_table",
    dataset_id=bq_dataset,
    table_id="test_table2",
    table_resource={
        "schema": {
            "fields": [
                {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
            ],
        },
    },
)

time_sensor = TimeDeltaSensor(
    task_id='wait_for_5minutes',
    delta=timedelta(minutes=5),
    timeout=600,
    dag=dag
)
delete_bucket=GCSDeleteBucketOperator(
                                      task_id='delete_bucket',
                                      bucket_name="airflowgcsbucket123456",
                                      dag=dag
                                      )
delete_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table=f"{proj_id}.{bq_dataset}.test_table2",
)
dummy_operator>>create_bucket>>list_objects>>create_table>>delete_table>>delete_bucket                                    