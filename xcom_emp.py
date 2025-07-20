from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args={'start_date':days_ago(0),}

dag=DAG(
        'xcom_example', 
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
        )
def push_data(**kwargs):
        kwargs['ti'].xcom_push(key='message',value='data loaded')
push_task=PythonOperator(
                         task_id='push_task',
                         python_callable=push_data,
                         provide_context=True,                         
                         dag=dag)
def pull_data(**kwargs):
    message=kwargs['ti'].xcom_pull(task_id='push_task',key='message')
    
pull_task=PythonOperator(
                        task_id='pull_task',
                        python_callable=pull_data,
                        provide_context=True,
                        dag=dag)
                        
push_task>>pull_task