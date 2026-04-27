from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Mevcut dosyanın (dag dosyası) bulunduğu tam yolu al
current_dir = os.path.dirname(os.path.abspath(__file__))
# Bir üst klasöre çık ve 'scripts' klasörüne bağlan
scripts_path = os.path.join(os.path.dirname(current_dir), "scripts")
# Bu yolu Python'ın arama listesine ekle
sys.path.insert(0, scripts_path)


from extract_energyzero import extract_energyzero
from transform_pandas import transform_energyzero

with DAG(
    dag_id="energyzero_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_energyzero,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=lambda **ctx: transform_energyzero(
            ctx["ti"].xcom_pull("extract")
        ),
    )

    extract >> transform