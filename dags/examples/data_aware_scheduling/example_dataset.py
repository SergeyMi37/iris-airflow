#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset

example_asset = Asset("s3://dataset/example.csv")

with DAG(
    dag_id="example_dataset_producer",
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule=None,
    tags=["asset"],
):
    # `example_dataset` may be updated by this upstream producer task.
    producer = EmptyOperator(
        task_id="producer",
        outlets=[example_asset],
    )

    # `example_dataset_consumer` DAG is executed before this `wait` task is completed.
    wait = BashOperator(
        task_id="wait",
        bash_command="sleep 10",
    )

    producer >> wait


# This DAG is triggered when `producer` task of `example_dataset_producer` DAG is completed successfully.
with DAG(
    dag_id="example_dataset_consumer",
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule=[example_asset],  # DAG can also require multiple assets. e.g., [asset1, asset2]
    tags=["asset"],
):
    EmptyOperator(task_id="consumer")
