import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.api_fetch import fetch_and_process_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    'bcra_data_pipeline',
    default_args=default_args,
    schedule_interval='0 7 * * 1-5', # At 07:00 on every day-of-week from Monday through Friday
    catchup=False
)

# Endpoints and Concepts
endpoints = [
    ("/plazo_fijo", "Plazos fijos (m)"),
    ("/depositos", "Depositos (m)"),
    ("/cajas_ahorro", "Cajas Ahorro (m)"),
    ("/cuentas_corrientes", "Cuentas corrientes (m)"),
    ("/usd", "Dolar blue"),
    ("/usd_of", "Dolar oficial")
]

# Task to fetch data from a single endpoint
def fetch_data(endpoint, description, **kwargs):
    df = fetch_and_process_data(endpoint, description)
    # Push the DataFrame to XComs
    kwargs['ti'].xcom_push(key=endpoint, value=df)

# Task to combine all DataFrames
def combine_dataframes(**kwargs):
    ti = kwargs['ti']
    dataframes = []
    
    for endpoint, description in endpoints:
        sanitized_endpoint = endpoint.replace("/", "_").replace("(", "").replace(")", "").replace(" ", "_")
        df = ti.xcom_pull(task_ids=f'fetch_data_{sanitized_endpoint}', key=endpoint)
        if df is not None and not df.empty:
            dataframes.append(df)
    
    if dataframes:
        # Concatenate all DataFrames into one
        final_df = pd.concat(dataframes, ignore_index=True)
        # Process the final DataFrame as needed, e.g., load it into a database
        print(final_df.head())
        return final_df
    else:
        raise ValueError("No dataframes were fetched. Final dataframe can't be created.")

# Dynamically create a task for each endpoint
for endpoint, description in endpoints:
    sanitized_endpoint = endpoint.replace("/", "_").replace("(", "").replace(")", "").replace(" ", "_")
    task = PythonOperator(
        task_id=f'fetch_data_{sanitized_endpoint}',
        python_callable=fetch_data,
        op_kwargs={'endpoint': endpoint, 'description': description},
        dag=dag,
    )

# Task to combine DataFrames
combine_task = PythonOperator(
    task_id='combine_dataframes',
    python_callable=combine_dataframes,
    dag=dag,
)

# Set the combine task to be dependent on all the fetch tasks
for endpoint, _ in endpoints:
    sanitized_endpoint = endpoint.replace("/", "_").replace("(", "").replace(")", "").replace(" ", "_")
    dag.get_task(f'fetch_data_{sanitized_endpoint}').set_downstream(combine_task)
