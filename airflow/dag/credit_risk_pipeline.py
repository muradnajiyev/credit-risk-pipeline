from datetime import datetime, timedelta
from airflow.sdk import dag, task_group, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.task.trigger_rule import TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook



def get_yesterday(logical_date):
    return (logical_date - timedelta(days=1)).date()


def check_row_count():
    context = get_current_context()
    yesterday = get_yesterday(context["logical_date"])

    config = Variable.get(
        "credit_pipeline_config",
        deserialize_json=True
    )

    table_name = config["postgres_table"]

    pg_hook = PostgresHook(postgres_conn_id="pg_conn_id")

    result = pg_hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE application_date = %s
        """,
        parameters=(yesterday,)
    )

    row_count = result[0]
    print(f"Dünənki sətir sayı: {row_count}")

    if row_count > 2:
        return "choose_spark_operator"

    return "fail_task"


def choose_spark():
    runner = Variable.get(
        "spark_runner",
        default_var="submit"
    )

    if runner == "submit":
        return "run_spark_submit"

    elif runner == "bash":
        return "run_spark_bash"

    raise ValueError(
        "spark_runner yalnız submit və ya bash daxil edin"
    )


def fail_pipeline():
    context = get_current_context()

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id

    raise AirflowFailException(
        f"DAG: {dag_id}, Task: {task_id} uğursuz oldu. "
        "Dünən üçün məlumat sayı 2 və ya daha azdır."
    )


SPARK_PACKAGES = (
    "org.postgresql:postgresql:42.7.3,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "io.delta:delta-spark_2.12:3.2.0"
)


@dag(
    dag_id="credit_risk_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    user_defined_macros={
        "get_yesterday": get_yesterday
    }
)
def credit_risk_pipeline():

    start = EmptyOperator(task_id="start")

    
    @task_group(group_id="check_data_group")
    def check_data_group():

        check_yesterday_data = SqlSensor(
            task_id="check_yesterday_data",
            conn_id="pg_conn_id",
            sql="""
                SELECT COUNT(*)
                FROM {{ var.json.credit_pipeline_config.postgres_table }}
                WHERE application_date = '{{ get_yesterday(logical_date) }}'
            """,
            mode="reschedule",
            poke_interval=10,
            timeout=60
        )

        check_blacklist_file = FileSensor(
            task_id="check_blacklist_file",
            fs_conn_id="fs_default",
            filepath="/opt/airflow/data/blacklist.csv",
            mode="reschedule",
            poke_interval=10,
            timeout=60
        )

    sensors = check_data_group()

    
    branch_on_row_count = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=check_row_count
    )

    choose_spark_operator = BranchPythonOperator(
        task_id="choose_spark_operator",
        python_callable=choose_spark
    )

    
    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=fail_pipeline
    )

    run_spark_submit = SparkSubmitOperator(
        task_id="run_spark_submit",
        application="/opt/airflow/spark_jobs/credit_risk_job.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        env_vars={
            "PG_USER": "{{ conn.pg_conn_id.login }}",
            "PG_PASSWORD": "{{ conn.pg_conn_id.password }}",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "{{ conn.minio_conn.login }}",
            "MINIO_SECRET_KEY": "{{ conn.minio_conn.password }}"
        }
    )

    run_spark_bash = BashOperator(
        task_id="run_spark_bash",
        bash_command=(
            "spark-submit "
            "--master 'local[*]' "
            f"--packages {SPARK_PACKAGES} "
            "/opt/airflow/spark_jobs/credit_risk_job.py"
        ),
        env={
            "PG_USER": "{{ conn.pg_conn_id.login }}",
            "PG_PASSWORD": "{{ conn.pg_conn_id.password }}",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "{{ conn.minio_conn.login }}",
            "MINIO_SECRET_KEY": "{{ conn.minio_conn.password }}"
        },
        append_env=True
    )

    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> sensors >> branch_on_row_count

    branch_on_row_count >> [
        choose_spark_operator,
        fail_task
    ]

    choose_spark_operator >> [
        run_spark_submit,
        run_spark_bash
    ]

    [
        sensors,
        branch_on_row_count,
        choose_spark_operator,
        run_spark_submit,
        run_spark_bash,
        fail_task
    ] >> end_task


credit_risk_pipeline()
