import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import pytz
from utils import read_api_credentials

def fetch_and_process_data(endpoint: str, description: str) -> pd.DataFrame:
    headers = {
#        'Authorization': f'Bearer {read_api_credentials("config/config.ini", "api_bcra")["api_token"]}'
         'Authorization': f'Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3Mzg3Mzk5NjAsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJ0b21hc21kaWxvcmV0b0Bob3RtYWlsLmNvbSJ9.A5YjLTY_TUZ_pAE-14cs9uDvPRS6q9wurCGXlHAc5DyDM-2RogMIB6f2JgUdCFrUu5ghevir33wSQ55vhdK8pA'
    }
    url = f'https://api.estadisticasbcra.com{endpoint}'
    response = requests.get(url, headers=headers)
    start = datetime.now(pytz.timezone('America/Buenos_Aires')) - timedelta(days=30)
    end = datetime.now(pytz.timezone('America/Buenos_Aires')) - timedelta(days=1)


    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        df.rename(columns={'d': 'Date', 'v': 'Value'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize('America/Buenos_Aires')
        df['Concept'] = description
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]
        
        return df
    
    else:
        print(f'Failed to fetch data from {endpoint}. Status code: {response.status_code}')
        return pd.DataFrame()  