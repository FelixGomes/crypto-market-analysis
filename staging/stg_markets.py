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

def get_markets(limit=100):
    """Extrai dados de mercados da CoinCap API"""
    
    url = f"{BASE_URL}/markets?limit={limit}&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao buscar mercados: {e}")
        return None

    try:
        data = response.json()['data']
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        # Rename columns to match database schema
        column_mapping = {
            'exchangeId': 'exchange_id',
            'baseSymbol': 'base_symbol',
            'baseId': 'base_id',
            'quoteSymbol': 'quote_symbol',
            'quoteId': 'quote_id',
            'priceQuote': 'price_quote',
            'priceUsd': 'price_usd',
            'volumeUsd24Hr': 'volume_usd_24hr',
            'percentExchangeVolume': 'percent_exchange_volume',
            'tradesCount24Hr': 'trades_count_24hr'
        }
        
        df = df.rename(columns=column_mapping)
        df['ingestion_timestamp'] = datetime.utcnow()
        return df
    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        return None

if __name__ == "__main__":
    df_markets = get_markets()
    if df_markets is not None:
        db.insert_dataframe(df_markets, table_name="stg_markets")