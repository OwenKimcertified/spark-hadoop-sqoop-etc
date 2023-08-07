from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


arg = {
    'start_date' : datetime(2023, 8, 2)
}

with DAG(dag_id = 'spark_airflow',
         schedule_interval = '@daily',
         default_args = arg,
         tags = ['spark_airflow'],
         catchup = False) as dag:

    # spark_sql = SparkSqlOperator(
    #     task_id = 'execute_spark_sql',
    #     sql = 'select * from riot_challenger_list',
    #     master = 'local'
    # )

    submit_job = SparkSubmitOperator(
        application = "C:/Users/admin/Desktop/spark_hadoop/data-engineering/01-spark/count_trips.py",
        task_id = 'test_job',
        conn_id = 'spark_local'
    )