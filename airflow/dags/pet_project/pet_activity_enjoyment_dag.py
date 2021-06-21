from airflow.models import DAG, Variable
# from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
# from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    'owner': 'Airflow',
}

with DAG(
        dag_id='pet_activity_enjoyment_data_product',
        default_args=args,
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['data', 'product', 'pet', 'activity'],
) as dag:

    pet_event_ingestion = SparkSubmitOperator(task_id='pet_event_ingestion',
                                              conn_id='spark_local',
                                              application="/home/airflow/.local/lib/python3.6/site-packages/pyspark/examples/src/main/python/pi.py",
                                              total_executor_cores=1,
                                              # packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
                                              executor_cores=1,
                                              executor_memory='1g',
                                              driver_memory='1g',
                                              name='pet_event_ingestion',
                                              execution_timeout=timedelta(minutes=10),
                                              )
pet_event_ingestion
# # [START howto_operator_spark_jdbc]
# jdbc_to_spark_job = SparkJDBCOperator(
#     cmd_type='jdbc_to_spark',
#     jdbc_table="foo",
#     spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
#     jdbc_driver="org.postgresql.Driver",
#     metastore_table="bar",
#     save_mode="overwrite",
#     save_format="JSON",
#     task_id="jdbc_to_spark_job",
# )
#
# spark_to_jdbc_job = SparkJDBCOperator(
#     cmd_type='spark_to_jdbc',
#     jdbc_table="foo",
#     spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
#     jdbc_driver="org.postgresql.Driver",
#     metastore_table="bar",
#     save_mode="append",
#     task_id="spark_to_jdbc_job",
# )
# # [END howto_operator_spark_jdbc]
#
# # [START howto_operator_spark_sql]
# sql_job = SparkSqlOperator(sql="SELECT * FROM bar", master="local", task_id="sql_job")
# # [END howto_operator_spark_sql]
