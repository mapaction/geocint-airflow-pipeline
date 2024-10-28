from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipline_lib.hdx_admin_boundary_scrape import download_zip_from_hdx
from pipline_lib.countires_iso import COUNTRIES

configs = COUNTRIES

with DAG(
    dag_id='extract_all_admin_boundaries',
    start_date=datetime(2024, 10, 7),
    schedule_interval=None,
    catchup=False,
    tags=["admin boundaries"],
    default_args={
        'retries': 1,
    },
) as dag:

    # Create a list to store tasks
    download_tasks = []

    for country_name, country_data in configs.items():
        iso3_code = country_data['code']
        download_task = PythonOperator(
            task_id=f'download_{iso3_code}',
            python_callable=download_zip_from_hdx,
            op_kwargs={
                'iso3_code': iso3_code,
                'base_folder': f'AllAdminBoundaries/{iso3_code}' 
            }
        )
        download_tasks.append(download_task)

    # Set dependencies for concurrent execution (using a sliding window)
    for i in range(len(download_tasks) - 5):  # Adjust 5 to control concurrency
        for j in range(5): 
            if i + j + 5 < len(download_tasks):
                download_tasks[i + j] >> download_tasks[i + j + 5] 