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

def get_asset_ids_from_db():
    """Busca os IDs dos assets já armazenados no banco"""
    query = "SELECT id FROM staging_layer.stg_assets"
    df = db.read_query(query)
    if df is not None:
        return ','.join(df['id'].tolist())
    return None

def get_rates():
    """Extrai dados de taxas de conversão da CoinCap API para os assets existentes"""
    
    # Busca os IDs dos assets no banco
    asset_ids = get_asset_ids_from_db()
    if not asset_ids:
        print("Nenhum asset encontrado na tabela stg_assets")
        return None
    
    url = f"{BASE_URL}/rates?ids={asset_ids}&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao buscar taxas: {e}")
        return None

    try:
        data = response.json()['data']
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        # Rename columns to match database schema
        column_mapping = {
            'currencySymbol': 'currency_symbol',
            'rateUsd': 'rate_usd'
        }
        
        df = df.rename(columns=column_mapping)
        df['ingestion_timestamp'] = datetime.utcnow()
        return df
    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        return None

if __name__ == "__main__":
    df_rates = get_rates()
    if df_rates is not None:
        db.insert_dataframe(df_rates, table_name="stg_rates")