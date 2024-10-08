from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipline_lib.create_pcode_dict import generate_pcode_json_file

with DAG(
    dag_id='generate_pcode_json_file',
    start_date=datetime(2024, 10, 7),
    schedule_interval=None,
    catchup=False,
    tags=["pcodes"],
    default_args={
        'retries': 1,
    },
) as dag:

    docker_worker_working_dir = "/opt/airflow"

    generate_task = PythonOperator(
        task_id=f'generate_pcode_json_file',
        python_callable=generate_pcode_json_file,
        op_kwargs={
            'shapefile_dir': f'{docker_worker_working_dir}/AllAdminBoundaries',
            'output_dir': f'{docker_worker_working_dir}/dags/static_data' 
        }
    )