from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def build_tasks(dag: DAG) -> tuple[BaseOperator, BaseOperator]:
    start_sw2 = EmptyOperator(task_id="start_sw2", dag=dag)
    end_sw2 = EmptyOperator(task_id="end_sw2", dag=dag)

    task1_sw2 = EmptyOperator(
        task_id="task1_sw2",
        dag=dag,
    )

    start_sw2 >> task1_sw2 >> end_sw2
    return start_sw2, end_sw2
