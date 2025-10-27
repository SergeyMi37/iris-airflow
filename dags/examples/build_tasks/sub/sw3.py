from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def build_tasks(dag: DAG) -> tuple[BaseOperator, BaseOperator]:
    start_sw3 = EmptyOperator(task_id="start_sw3", dag=dag)
    end_sw3 = EmptyOperator(task_id="end_sw3", dag=dag)

    task1_sw3 = EmptyOperator(
        task_id="task1_sw3",
        dag=dag,
    )
    task2_sw3 = EmptyOperator(
        task_id="task2_sw3",
        dag=dag,
    )

    start_sw3 >> task1_sw3 >> task2_sw3 >> end_sw3
    return start_sw3, end_sw3
