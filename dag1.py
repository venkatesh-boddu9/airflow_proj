from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args={
               'start_date':days_ago(0),}
dag1=DAG(
         'dag-1',
          default_args=default_args,
          description='dag1 for trigger dag example',
          schedule_interval='@once',
          catchup=False          
          )
dummy_start=DummyOperator(task_id='dummy_start')
echo_task=BashOperator(
                         task_id='echo',
                         bash_command='echo hello this is dag1',
                         dag=dag1
                         ) 
trigger_task=TriggerDagRunOperator(
                                   task_id='dag2_task',
                                   trigger_dag_id='dag-2',
                                   dag=dag1
                                   )
dummy_stop=DummyOperator(task_id='dummy_stop')
dummy_start>>echo_task>>trigger_task>>dummy_stop                                  