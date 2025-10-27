from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def build_tasks(dag: DAG) -> tuple[BaseOperator, BaseOperator]:
    start_sw1 = EmptyOperator(task_id="start_sw1", dag=dag)
    end_sw1 = EmptyOperator(task_id="end_sw1", dag=dag)

    task1_sw1 = EmptyOperator(
        task_id="task1_sw1",
        dag=dag,
    )

    start_sw1 >> task1_sw1 >> end_sw1
    return start_sw1, end_sw1
