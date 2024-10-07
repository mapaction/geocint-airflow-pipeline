from airflow import DAG
from airflow.operators.python import PythonOperator
from pipline_lib.countires_iso import COUNTRIES
from pipline_lib import hdx_admin_boundary_scrape
import pendulum

configs = COUNTRIES

def extract_all_admin_boundaries():
    for country_name, config  in configs.items():
        country_iso = config['code']
        directory = f"AllAdmnBoundaries/{country_iso}"
        hdx_admin_boundary_scrape.download_zip_from_hdx(country_iso, directory)

with DAG (
    dag_id="extract_all_admin_boundaries",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['all_admin_boundaries'],
) as dag:
    # Extract administrative boundaries
    extract_boundaries = PythonOperator(
        task_id='extract_boundaries_task',
        python_callable=extract_all_admin_boundaries
    )

