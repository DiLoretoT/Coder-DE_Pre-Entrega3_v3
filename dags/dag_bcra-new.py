import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.api_fetch import fetch_and_process_data
from scripts.utils import connect_to_db, load_to_sql

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


# def create_redshift_connection(**kwargs):
#     print("Creating Redshift connection...")
#     engine = connect_to_db("config/config.ini", "redshift")
#     # Store the engine in XComs for other tasks to use
#     kwargs['ti'].xcom_push(key='redshift_engine', value=engine)

# def load_to_redshift(**kwargs):
#     ti = kwargs['ti']
#     # Retrieve the combined DataFrame from XComs
#     final_df = ti.xcom_pull(task_ids='combine_dataframes')
#     if final_df is not None and not final_df.empty:
#         # Retrieve the Redshift engine from XComs
#         engine = ti.xcom_pull(key='redshift_engine')
#         with engine.connect() as conn, conn.begin():
#             conn.execute("TRUNCATE TABLE tomasmartindl_coderhouse.stg_bcra")
#             load_to_sql(final_df, "stg_bcra", conn, "append")
            
#             conn.execute("""
#                 MERGE INTO tomasmartindl_coderhouse.bcra
#                 USING tomasmartindl_coderhouse.stg_bcra AS stg
#                 ON tomasmartindl_coderhouse.bcra.date = stg.date
#                 AND tomasmartindl_coderhouse.bcra.concept = stg.concept
#                 WHEN MATCHED THEN
#                 update tomasmartindl_coderhouse.bcra.value = stg.value
#                 WHEN NOT MATCHED THEN
#                 INSERT (tomasmartindl_coderhouse.bcra.date, tomasmartindl_coderhouse.bcra.concept, tomasmartindl_coderhouse.bcra.value)
#                 VALUES (stg.date, stg.concept, stg.value)             
#             """)

#         print("Data successfully loaded to Redshift.")
#     else:
#         raise ValueError("No DataFrame to load to Redshift.")


def load_to_redshift_combined(**kwargs):
    # Assuming config_file_path is stored in Airflow Variables
    ti = kwargs['ti']
    #config_file_path = Variable.get("config_file_path")
    final_df = ti.xcom_pull(task_ids='combine_dataframes')

    print("Creating Redshift connection...")
    engine = connect_to_db("config/config.ini", "redshift")
    if engine is None:
        raise ValueError("Failed to create database engine.")

    
    if final_df is not None and not final_df.empty:
        with engine.connect() as conn, conn.begin():
            conn.execute("TRUNCATE TABLE tomasmartindl_coderhouse.stg_bcra")
            load_to_sql(final_df, "stg_bcra", conn, "append")
            
            conn.execute("""
                MERGE INTO tomasmartindl_coderhouse.bcra AS target
                USING (SELECT date, concept, value FROM tomasmartindl_coderhouse.stg_bcra) AS source
                ON target.date = source.date AND target.concept = source.concept
                WHEN MATCHED THEN
                UPDATE SET value = source.value
                WHEN NOT MATCHED THEN
                INSERT (date, concept, value)
                VALUES (source.date, source.concept, source.value)
            """)

            # conn.execute("""
            #     INSERT INTO tomasmartindl_coderhouse.bcra (date, concept, value)
            #     SELECT date, concept, value FROM tomasmartindl_coderhouse.stg_bcra
            #     ON CONFLICT (date, concept) 
            #     DO UPDATE SET value = EXCLUDED.value;
            # """)

        print("Data successfully loaded to Redshift.")
    else:
        raise ValueError("No DataFrame to load to Redshift.")

# Task to load data to Redshift, now including connection creation
load_to_redshift_combined_task = PythonOperator(
    task_id='load_to_redshift_combined',
    python_callable=load_to_redshift_combined,
    provide_context=True,
    dag=dag,
)


# # Task to create Redshift connection
# create_connection_task = PythonOperator(
#     task_id='create_redshift_connection',
#     python_callable=create_redshift_connection,
#     provide_context=True,
#     dag=dag,
# )

# # Task to load data to Redshift
# load_to_redshift_task = PythonOperator(
#     task_id='load_to_redshift',
#     python_callable=load_to_redshift,
#     provide_context=True,
#     dag=dag,
# )

# Define task dependencies
#combine_task.set_downstream(create_connection_task)
#create_connection_task.set_downstream(load_to_redshift_task)

# Define task dependencies
combine_task >> load_to_redshift_combined_task
