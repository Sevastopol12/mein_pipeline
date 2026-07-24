from pendulum import datetime
from airflow.sdk import DAG
from tasks import run_extract


with DAG(
    dag_id="checkin_pipeline",
    schedule="@daily",
    start_date=datetime(year=2026, month=7, day=20, hour=0,  minute=0),
    catchup=True,
    max_active_runs=1
) as dag:
    
    run_extract()
