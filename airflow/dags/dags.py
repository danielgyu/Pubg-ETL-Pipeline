from datetime import datetime, timedelta

from airflow                                       import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash                        import BashOperator
from airflow.operators.python                      import PythonOperator
from airflow.operators.dummy_operator              import DummyOperator
from airflow.utils.dates                           import days_ago
from airflow.models                                import Variable
from emr_helpers                                   import run_spark_step, check_emr_cluster
from operators.load_warehouse                      import LoadWarehouseOperator
from operators.quality_check                       import DataQualityCheckOperator

s3_bucket = 's3://capstone-pubg'
s3_key = '/staging'
access_key = Variable.get('access_key')
secret_key = Variable.get('secret_key')

default_args = {
    'owner': 'KG lee',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

dag = DAG(
    'capstone-pubg-etl',
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval='0 * * * * '
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

emr_step_task = PythonOperator(
    task_id='emr_step_spark',
    python_callable=run_spark_step,
    op_kwargs={'AWS_ACCESS_KEY': access_key,
               'AWS_SECRET_KEY': secret_key},
    dag=dag
)

check_emr_cluster = PythonOperator(
    task_id='check_emr_cluster',
    python_callable=check_emr_cluster,
    op_kwargs={'AWS_ACCESS_KEY': access_key,
               'AWS_SECRET_KEY': secret_key},
    dag=dag
)

create_tables = open('plugins/create_redshift.sql', 'r')
create_redshift_task = PostgresOperator(
    task_id='creat_redshift_tables',
    postgres_conn_id='redshift',
    sql=create_tables.read(),
    dag=dag
)
create_tables.close()

load_matches_task = LoadWarehouseOperator(
    task_id='load_dimension_matches',
    postgres_conn_id='redshift',
    aws_access_key=access_key,
    aws_secret_key=secret_key,
    s3_bucket=s3_bucket,
    s3_key="{}/match".format(s3_key),
    table='matches',
    truncate=True,
    dag=dag
)

load_performances_task = LoadWarehouseOperator(
    task_id='load_dimension_performance',
    postgres_conn_id='redshift',
    aws_access_key=access_key,
    aws_secret_key=secret_key,
    s3_bucket=s3_bucket,
    s3_key="{}/performance".format(s3_key),
    table='performances',
    truncate=True,
    dag=dag
)

load_dates_task = LoadWarehouseOperator(
    task_id='load_dimension_dates',
    postgres_conn_id='redshift',
    aws_access_key=access_key,
    aws_secret_key=secret_key,
    s3_bucket=s3_bucket,
    s3_key="{}/date".format(s3_key),
    table='dates',
    truncate=True,
    dag=dag
)

load_details_task = LoadWarehouseOperator(
    task_id='load_dimension_details',
    postgres_conn_id='redshift',
    aws_access_key=access_key,
    aws_secret_key=secret_key,
    s3_bucket=s3_bucket,
    s3_key="{}/detail".format(s3_key),
    table='details',
    truncate=True,
    dag=dag
)

load_locations_task = LoadWarehouseOperator(
    task_id='load_dimension_locations',
    postgres_conn_id='redshift',
    aws_access_key=access_key,
    aws_secret_key=secret_key,
    s3_bucket=s3_bucket,
    s3_key="{}/location".format(s3_key),
    table='locations',
    truncate=True,
    dag=dag
)

check_redshift_task = DataQualityCheckOperator(
    task_id='check_tables',
    postgres_conn_id='redshift',
    tables=['details', 'dates', 'locations', 'performances', 'matches'],
    columns=['detail_id', 'game_time', 'location_id', 'performance_id', 'match_id'],
    expected_result=0.0,
    dag=dag
)

end_operator = DummyOperator(task_id='end_execution', dag=dag)


start_operator       >> emr_step_task
emr_step_task        >> check_emr_cluster
check_emr_cluster    >> create_redshift_task
create_redshift_task >> [load_dates_task, load_locations_task, load_details_task,
                      load_performances_task, load_matches_task] \
                     >> check_redshift_task
check_redshift_task  >> end_operator
