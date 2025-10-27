from airflow.sdk import dag, task
from datetime import datetime, timedelta
from pprint import pprint
from airflow.models import DagRun
import pendulum
from pendulum.datetime import DateTime as DateTime

@dag(
    dag_id="wikipedia_usage_data_ingestion", 
    start_date=datetime(2025, 10, 1),
    schedule=timedelta(hours=6), 
    catchup=False
)
def wikipedia_usage_data_ingestion():
    @task
    def fetch_data(dag_run:DagRun ):
        import requests
        import os
        import io
        import gzip
        from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        wiki_url = "https://dumps.wikimedia.org/other/pageviews/"
        print(dag_run.logical_date)
        dt:DateTime = pendulum.instance(dag_run.logical_date)
        extract_hours:list[DateTime] = [dt.subtract(hours=i) for i in range(12, 11, -1)]
        #pageview_timestamps = [f"{t.}" for t in extract_hours]
        print(type(dt))
        pageview_timestamps  = [ f"{t.year}/{t.year}-{t.month}/pageviews-{t.format('YYYYMMDD-HH')}0000.gz" for t in extract_hours]
        print(pageview_timestamps)
        for ts in pageview_timestamps:
            url = f"{wiki_url}{ts}"
            print("url is ", url)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.content
                # with open( os.path.join("/opt/airflow/logs", ts.split("/")[-1]), 'wb') as f:
                #      f.write(data)
                zip_file = io.BytesIO(data)
                s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
                bucket_name = 'k.rajnarayanan.spark'
                key = f"raw/{ts.split("/")[-1]}" # replace with your bucket name
                s3_hook.load_bytes(
                    bytes_data=zip_file.getvalue(),
                    key=key,
                    bucket_name=bucket_name,
                    replace=True
                )
                print(f"Uploaded {key} to S3 bucket {bucket_name}")

                with gzip.open(zip_file, mode='rb') as f:
                    file_content = f.read()
                    print(file_content[:100])
                    # with open( os.path.join("/opt/airflow/logs", f"{ts.split("/")[-1]}.json"), 'wb') as f:
                    #  f.write(file_content)
                

        #pprint(dag_run.logical_date)
#https://dumps.wikimedia.org/other/pageviews
        # import requests
        # url = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/2023/09/01"
        # response = requests.get(url)
        # response.raise_for_status()
        # data = response.json()
        # return data['items'][0]['articles']

    # @task
    # def process_data(articles):
    #     processed = []
    #     for article in articles:
    #         processed.append({
    #             'article': article['article'],
    #             'views': article['views'],
    #             'rank': article['rank']
    #         })
    #     return processed

    # @task
    # def store_data(processed_data):
    #     import sqlite3
    #     conn = sqlite3.connect('wikipedia_usage.db')
    #     c = conn.cursor()
    #     c.execute('''
    #         CREATE TABLE IF NOT EXISTS usage (
    #             article TEXT,
    #             views INTEGER,
    #             rank INTEGER,
    #             date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #         )
    #     ''')
    #     for entry in processed_data:
    #         c.execute('''
    #             INSERT INTO usage (article, views, rank) VALUES (?, ?, ?)
    #         ''', (entry['article'], entry['views'], entry['rank']))
    #     conn.commit()
    #     conn.close()

    articles = fetch_data()
    #processed_data = process_data(articles)
    #store_data(processed_data)
dag_instance = wikipedia_usage_data_ingestion()
