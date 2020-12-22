from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


S3_BUCKET = 'udacity-dend'
S3_SONG_KEY = 'song_data'
S3_LOG_KEY = 'log_data/{{execution_date.year}}/{{execution_date.month}}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'dag'

default_args = {
    'owner': 'Faris alsaleem',
    'start_date': datetime(2018, 11, 1),
    'email': ['faris.al.saleem15@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG('sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_events",
    table_schema=SqlQueries.staging_events_schema,
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    region=REGION,
    data_format=f"JSON '{LOG_JSON_PATH}'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_songs",
    table_schema=SqlQueries.staging_songs_schema,
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    region=REGION,
    data_format="JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='songplays',
    delete_load=True,
    table_schema=SqlQueries.songplay_table_schema,
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='users',
    table_schema=SqlQueries.user_table_schema,
    sql_statement=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='songs',
    delete_load=True,
    table_schema=SqlQueries.song_table_schema,
    sql_statement=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='artists',
    delete_load=True,
    table_schema=SqlQueries.artist_table_schema,
    sql_statement=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='time',
    delete_load=True,
    table_schema=SqlQueries.time_table_schema,
    sql_statement=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# Begin_execution downstream to stage_events & stage_songs that downstream to Load_songplays_fact_table
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

# Load_songplays_fact_table downstream to
# Load_user_dim_table, Load_song_dim_table Load_artist_dim_table and Load_time_dim_table that downstream to
# Run_data_quality_checks
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks


# Run_data_quality_checks downstream to Stop_execution
run_quality_checks >> end_operator

