#THis airfow pipeline gets the top 5 most popular videos thumbnails on youtube and saves them in a temporary directory
import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _get_pictures():
    pathlib.Path("/tmp/thumbs").mkdir(parents=True, exist_ok=True)
    with open("/tmp/yt.json") as f:
        videos = json.load(f)
        image_urls = [video['snippet']['thumbnails']['default']['url'] for video in videos['items']]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-2]+".jpg"
                target_file = f"/tmp/thumbs/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

dag = DAG(
    dag_id="download_videos_thumbs",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)

download_videos = BashOperator(
    task_id="download_videos",
    bash_command="curl -o /tmp/yt.json -L 'https://youtube.googleapis.com/youtube/v3/videos?part=id%2Csnippet&chart=mostPopular&key=[ApiKey]' --header 'Accept: application/json' --compressed",
    dag=dag
)

get_pictures = PythonOperator( 
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)


download_videos >> get_pictures
