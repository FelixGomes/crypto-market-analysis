import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))
from utils.db_utils import PostgresHandler

# Carrega variáveis .env 
load_dotenv()
db = PostgresHandler()

API_KEY = os.getenv("COINCAP_API_KEY")
BASE_URL = "https://rest.coincap.io/v3"

def get_asset_markets(asset_id, limit=100):
    """Extrai dados de mercado para um asset específico"""
    
    url = f"{BASE_URL}/assets/{asset_id}/markets?limit={limit}&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao buscar mercados para {asset_id}: {e}")
        return None

    try:
        data = response.json()['data']
        if not data:  # Se não houver dados
            return None
            
        df = pd.DataFrame(data)
        # Add asset_id and timestamp
        df['asset_id'] = asset_id
        df['ingestion_timestamp'] = datetime.utcnow()
        return df
    except Exception as e:
        print(f"Erro ao processar dados para {asset_id}: {e}")
        return None

def get_all_assets_markets():
    """Busca todos os assets da tabela stg_assets e extrai seus mercados"""
    
    # Busca todos os asset_ids usando o método da classe PostgresHandler
    assets_df = db.get_distinct_values("id", "stg_assets")
    
    if assets_df is None:
        print("Não foi possível buscar os assets")
        return None

    all_markets = []
    for asset_id in assets_df['id']:
        print(f"Buscando mercados para {asset_id}...")
        markets_df = get_asset_markets(asset_id)
        
        if markets_df is not None:
            all_markets.append(markets_df)
            
        time.sleep(0.5)  # Rate limiting para evitar muitas requisições

    if all_markets:
        return pd.concat(all_markets, ignore_index=True)
    return None

if __name__ == "__main__":
    df_markets = get_all_assets_markets()
    if df_markets is not None:
        db.insert_dataframe(df_markets, table_name="stg_asset_markets")