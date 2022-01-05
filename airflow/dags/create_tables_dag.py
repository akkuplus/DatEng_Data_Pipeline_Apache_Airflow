from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,               # DAG does not have dependencies on past runs
    'retries': 3,                           # On failure, the task are retried 3 times
    'catchup_by_default': False,            # Catchup is turned off
    'email_on_retry': False                 # Do not email on retry
}

# Setup DAG
with DAG(
    dag_id="Create_tables_dag",
    default_args=default_args,
    description='Create tables in Redshift',    
    start_date=datetime.today(),
    schedule_interval="@once"
) as dag:
    create_tables_task = PostgresOperator(
        postgres_conn_id="redshift",
        task_id="create_tables",
        sql = getattr(SqlQueries, "create_tables")
    )
        
# Task dependencies
create_tables_task
