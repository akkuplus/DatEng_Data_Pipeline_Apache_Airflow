from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 1, 0,0,0),
    'end_date': datetime(2021, 1, 1, 2,0,0),
    'depends_on_past': False,               # DAG does not have dependencies on past runs
    'retries': 3,                           # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5),    # Retries happen every 5 minutes
    'catchup_by_default': False,            # Catchup is turned off
    'email_on_retry': False,                # Do not email on retry
    'max_active_runs_per_dag': 6
}


dag = DAG('ELT_dag',
          default_args=default_args,
          description='Load and transform data in Redshift',
          schedule_interval='0 * * * *',    # hourly execution
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


end_operator = DummyOperator(task_id='End_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='dev.public.staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    json_path = 's3://udacity-dend/log_json_path.json',
    use_paritioning = False,
    execution_date = '{{ execution_date }}',
    truncate_table=True
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='dev.public.staging_songs',
    s3_bucket='s3://udacity-dend/song_data',
    json_path = 'auto',
    use_paritioning = False,
    execution_date = '{{ execution_date }}',
    truncate_table=True
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    query_name="songplay_table_insert",
    do_append=False,
    execution_date = '{{ execution_date }}',
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    query_name="user_table_insert",
    do_append=False,
    execution_date = '{{ execution_date }}',    
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    query_name="song_table_insert",
    do_append=False,
    execution_date = '{{ execution_date }}',
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    query_name="artist_table_insert",
    do_append=False,
    execution_date = '{{ execution_date }}',
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    query_name="time_table_insert",
    do_append=False,
    execution_date = '{{ execution_date }}',
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "users", "songs", "artists", "time"],
    query_name="count_table_rows",
    min_number_of_rows = 1,
)


# Task dependencies - ETL steps

start_operator >> stage_events_to_redshift >> load_songplays_table # FROM staging_events LEFT JOIN staging_songs
start_operator >> stage_songs_to_redshift >> load_songplays_table # FROM staging_events LEFT JOIN staging_songs

load_songplays_table >> (load_time_dimension_table, load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table) >> run_quality_checks >> end_operator