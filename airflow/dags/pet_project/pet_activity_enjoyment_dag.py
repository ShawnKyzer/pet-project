from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

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
    # [START howto_operator_spark_submit]
    submit_job = SparkSubmitOperator(
        application="${SPARK_HOME}/examples/src/main/python/pi.py", task_id="submit_job"
    )
    # [END howto_operator_spark_submit]

    # [START howto_operator_spark_jdbc]
    jdbc_to_spark_job = SparkJDBCOperator(
        cmd_type='jdbc_to_spark',
        jdbc_table="foo",
        spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
        jdbc_driver="org.postgresql.Driver",
        metastore_table="bar",
        save_mode="overwrite",
        save_format="JSON",
        task_id="jdbc_to_spark_job",
    )

    spark_to_jdbc_job = SparkJDBCOperator(
        cmd_type='spark_to_jdbc',
        jdbc_table="foo",
        spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
        jdbc_driver="org.postgresql.Driver",
        metastore_table="bar",
        save_mode="append",
        task_id="spark_to_jdbc_job",
    )
    # [END howto_operator_spark_jdbc]

    # [START howto_operator_spark_sql]
    sql_job = SparkSqlOperator(sql="SELECT * FROM bar", master="local", task_id="sql_job")
    # [END howto_operator_spark_sql]
