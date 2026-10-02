import airflow
from airflow.operators.empty import EmptyOperator
from airflow import models
from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,

)

default_args = {
    'email': ['manojkumarsep43@gmail.com'],
    'start_date': datetime(2018, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1

}
with models.DAG(
        'advanced_loop',
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=None) as dag:
    my_table_list = ['test_1','test_2','test_3']
    # create_tables = []
    # start_tasks = []
    # end_tasks = []
    dummy_1 = EmptyOperator(
        task_id='dummy_start',
    )
    dummy_2 = EmptyOperator(
        task_id='join_tasks',
    )
    for table_id in my_table_list:
        start = EmptyOperator(
            task_id=f'start_{table_id}',
            trigger_rule='all_success'
        )
        # start_tasks.append(start)
        end = EmptyOperator(
            task_id=f'end_{table_id}',
            trigger_rule='all_success'
        )
        # end_tasks.append(end)
        create_table = BigQueryCreateTableOperator(
            task_id=f"Create_{table_id}",
            dataset_id="mydataset",
            table_id=table_id,
            table_resource={"schema": {"fields": [{"name": "NAME", "type": "STRING", "mode": "NULLABLE"}, {"name": "AGE", "type": "INTEGER", "mode": "NULLABLE"}, ]}}
            

        )
        dummy_1 >> start >> create_table >> end >> dummy_2