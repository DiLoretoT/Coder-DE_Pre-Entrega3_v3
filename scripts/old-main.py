# import requests
# import pandas as pd
# import pytz
# import psycopg2
# from sqlalchemy import create_engine
# #from sqlalchemy.orm import sessionmaker
# from datetime import datetime, timedelta
# from utils import read_api_credentials, connect_to_db, load_to_sql

# # AUTENTICACIÓN
# api_token2 = read_api_credentials("config/config.ini", "api_bcra")

# headers = {
#     'Authorization': f'Bearer {api_token2['api_token']}'
# }

# # OBTENCIÓN Y PREPARACIÓN DE DATAFRAME
# def consolidate(endpoint, description):
#     """
#     Esta función obtiene datos de un endpoint de la API y los convierte en un DataFrame de pandas.

#     Args:
#         endpoint (str): El endpoint de la API desde donde se extraen los datos.
#         concept (str): El concepto o categoría de los datos (ej. 'plazo fijo').

#     Returns: 
#         DataFrame: Un DataFrame con los datos obtenidos del endpoint de la API, o un dataframe vacío en caso de error.
#     """
#     url = f'https://api.estadisticasbcra.com{endpoint}'
#     response = requests.get(url, headers=headers)
#     #start = datetime.now(pytz.timezone('America/Buenos_Aires')) - timedelta(days=30)
#     #end = datetime.now(pytz.timezone('America/Buenos_Aires')) - timedelta(days=1)
    
#     if response.status_code == 200:
#         print(f'Status code: {response.status_code}')         
#         data = response.json()        
#         df = pd.DataFrame(data)        
#         df.rename(columns={'d': 'Date', 'v': 'Value'}, inplace= True)        
#         df['Date'] = pd.to_datetime(df['Date'])
#         df['Date'] = df['Date'].dt.tz_localize('America/Buenos_Aires')        
#         df['Concept'] = description        
#         #filtered_df = df[(df['Date'] >= start) & (df['Date'] <= end)]                
#         return df
                
#     else: 
#         print(f'Failed to fetch data from {endpoint}. Status code:', response.status_code)
#         # Retorna un df vacío
#         return pd.DataFrame()
        
# ## Endpoints y Concepts
# endpoints = [
#     ("/plazo_fijo", "Plazos fijos (m)"),
#     ("/depositos", "Depositos (m)"),
#     ("/cajas_ahorro", "Cajas Ahorro (m)"),
#     ("/cuentas_corrientes", "Cuentas corrientes (m)"),
#     ("/usd", "Dolar blue"),
#     ("/usd_of", "Dolar oficial")
# ]
# ## Lista vacía de endpoints para alojar durante el for
# dataframes = []

# ## Loop "for" que itera sobre la lista de tuplas, llamando a la función "consolidate" para obtener los df y agregarlos a la lista "dataframes" (siempre que la respuesta no sea None o un df vacío) 
# for endpoint, description in endpoints:
#     df = consolidate(endpoint, description)
#     if df is not None and not df.empty:
#         dataframes.append(df)
        

# ## Unificación de dataframes, generando un index nuevo y ordenando las columnas. Si no se obtuvo información, se arroja un mensaje que lo comenta. 
# if dataframes:
#     df_final = pd.concat(dataframes, ignore_index=True)
#     df_final = df_final[['Date', 'Concept', 'Value']]
#     print(df_final.head())
# else: 
#     print("No se lograron recolectar datos de los endpoints.")
    
# # CONNECTION TO REDSHIFT
# print("Creando conexión a Redshift...")
# engine = connect_to_db("config2.ini", "redshift")
# print(type(engine))
# print(type(engine.connect()))

# with engine.connect() as conn, conn.begin():

#     conn.execute("TRUNCATE TABLE tomasmartindl_coderhouse.stg_bcra")

#     load_to_sql(df_final, "stg_bcra", conn, "append")
    
#     conn.execute("""
#         MERGE INTO tomasmartindl_coderhouse.bcra
#         USING tomasmartindl_coderhouse.stg_bcra AS stg
#         ON tomasmartindl_coderhouse.bcra.date = stg.date
#         AND tomasmartindl_coderhouse.bcra.concept = stg.concept
#         WHEN MATCHED THEN
#         update tomasmartindl_coderhouse.bcra.value = stg.value
#         WHEN NOT MATCHED THEN
#         INSERT (tomasmartindl_coderhouse.bcra.date, tomasmartindl_coderhouse.bcra.concept, tomasmartindl_coderhouse.bcra.value)
#         VALUES (stg.date, stg.concept, stg.value)             
#     """)
    
#     #Debug print: si el código SQL se ejecuta sin excepciones, muestra el mensaj de éxito. 
#     print("La inserción de datos a PRD validando duplicados, fue lograda con éxito.")



# print("Conexión cerrada. Proceso finalizado")