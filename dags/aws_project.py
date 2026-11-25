import os
import sys
# So files in dags folder can access folder in it's parent directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.bronze_layer import BronzeLayer
from airflow.sdk import dag, task
from datetime import timedelta

@dag(
    schedule = '@daily',
    is_paused_upon_creation= False
)
def aws_project():
    @task.python(retries = 3, retry_delay=timedelta(minutes=5))
    def extract_load():
        #Fetching data from API and loading it to S3 using loop
        urls = ["https://raw.githubusercontent.com/arpanarora227/ApacheAirflowData/refs/heads/main/bookings.csv",
                "https://raw.githubusercontent.com/arpanarora227/ApacheAirflowData/refs/heads/main/airports.csv",
                "https://raw.githubusercontent.com/arpanarora227/ApacheAirflowData/refs/heads/main/passengers.csv"]

        obj = BronzeLayer()
        for url in urls:
            fetched_data = obj.ingest_data_api(url)
            obj.put_data_s3("airflowawsarpan", f"bronze/{url.split('/')[-1]}", fetched_data)

    # Task Dependency
    extract_load()

aws_project_dag = aws_project()