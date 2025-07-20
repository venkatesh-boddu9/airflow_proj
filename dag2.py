from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args={
               'start_date':days_ago(0),}
dag2=DAG(
         'dag-2',
          default_args=default_args,
          description='dag1 for trigger dag example',
          schedule_interval='@once',
          catchup=False          
          )
dummy_start=DummyOperator(task_id='dummy_start')
echo_task=BashOperator(
                         task_id='echo',
                         bash_command='echo hello this is dag2',
                         dag=dag2
                         ) 

dummy_stop=DummyOperator(task_id='dummy_stop')

dummy_start>>echo_task>>dummy_stop                                  