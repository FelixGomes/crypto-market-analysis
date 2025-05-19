import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))
from utils.db_utils import PostgresHandler

# Carrega variáveis .env 
load_dotenv()
db = PostgresHandler()

API_KEY = os.getenv("COINCAP_API_KEY")
BASE_URL = "https://rest.coincap.io/v3"

def get_exchanges(limit=100):
    """Extrai dados de exchanges da CoinCap API"""
    
    url = f"{BASE_URL}/exchanges?limit={limit}&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao buscar exchanges: {e}")
        return None

    try:
        data = response.json()['data']
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        # Rename columns to match database schema
        column_mapping = {
            'exchangeId': 'id',
            'percentTotalVolume': 'percent_total_volume',
            'volumeUsd': 'volume_usd',
            'tradingPairs': 'trading_pairs',
            'exchangeUrl': 'exchange_url',
            'socket': 'socket',
            'updated': 'updated'
        }
        
        df = df.rename(columns=column_mapping)
        df['ingestion_timestamp'] = datetime.utcnow()
        return df
    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        return None

if __name__ == "__main__":
    df_exchanges = get_exchanges()
    if df_exchanges is not None:
        db.insert_dataframe(df_exchanges, table_name="stg_exchanges")