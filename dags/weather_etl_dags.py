from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from weather_etl_pipeline import fetch_weather_data, plot_temperature_trends, load_to_db, get_db_engine, CITIES

default_args = {
    'owner': 'Lethabo',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl',
    default_args=default_args,
    description='ETL DAG for fetching weather data and storing in PostgreSQL',
    schedule_interval='0 * * * *',
    catchup=False
)

def etl_task_for_city(city):
    engine = get_db_engine()
    try:
        df = fetch_weather_data(city)
        plot_temperature_trends(df, city)
        load_to_db(df, engine)
    finally:
        engine.dispose()

for city in CITIES:
    PythonOperator(
        task_id=f'weather_etl_{city.lower()}',
        python_callable=etl_task_for_city,
        op_args=[city],
        dag=dag
    )
