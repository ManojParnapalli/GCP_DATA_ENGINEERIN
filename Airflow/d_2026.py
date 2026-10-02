from datetime import datetime

from airflow import models
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryInsertJobOperator,
)


default_args = {
    "email": ["manojkumarsep43@gmail.com"],
    "start_date": datetime(2018, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}


with models.DAG(
    dag_id="my_first_load_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="First_dataset_new",
        gcp_conn_id="new_bigquery_conn",
    )

    create_my_table = BigQueryCreateTableOperator(
        task_id="create_my_table",
        dataset_id="First_dataset_new",
        table_id="mydata_table",
        gcp_conn_id="new_bigquery_conn",
        table_resource={
            "schema": {
                "fields": [
                    {
                        "name": "NAME",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "AGE",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                    },
                ]
            }
        },
    )

    bqrunner1 = BigQueryInsertJobOperator(
        task_id="load_into_TMP_TABLE1",
        gcp_conn_id="new_bigquery_conn",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `light-mountain-371707.practise.test`
                    SELECT *
                    FROM `light-mountain-371707.practise.test2`
                """,
                "useLegacySql": False,
            }
        },
    )

    bqrunner2 = BigQueryInsertJobOperator(
        task_id="load_into_TMP_TABLE2",
        gcp_conn_id="new_bigquery_conn",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `light-mountain-371707.practise.test2`
                    SELECT *
                    FROM `light-mountain-371707.practise.test`
                """,
                "useLegacySql": False,
            }
        },
    )

    start >> create_dataset >> create_my_table >> [bqrunner1, bqrunner2] >> end

