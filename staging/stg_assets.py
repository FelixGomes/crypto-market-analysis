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
db_writer = PostgresHandler()

API_KEY = os.getenv("COINCAP_API_KEY")
BASE_URL = "https://rest.coincap.io/v3"

def get_top_assets(limit=20):
    """Extrai os top 20 assets da CoinCap API"""

    url = f"{BASE_URL}/assets?limit={limit}&apiKey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro: {e}")
        return None

    try:
        data = response.json()['data']
        df = pd.DataFrame(data)
        
        # Rename columns to match database schema
        column_mapping = {
            'maxSupply': 'max_supply',
            'marketCapUsd': 'market_cap_usd',
            'volumeUsd24Hr': 'volume_usd_24hr',
            'priceUsd': 'price_usd',
            'changePercent24Hr': 'change_percent_24hr',
            'vwap24Hr': 'vwap_24hr'
        }
        
        df = df.rename(columns=column_mapping)
        df['ingestion_timestamp'] = datetime.utcnow()
        return df
    except Exception as e:
        print(f"Erro: {e}")
        return None

if __name__ == "__main__":
    df_assets = get_top_assets()
    if df_assets is not None:
        db_writer.insert_dataframe(df_assets, table_name="stg_assets")

