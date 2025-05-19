import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

from staging.stg_assets import get_top_assets
from staging.stg_exchanges import get_exchanges
from staging.stg_assets_markets import get_all_assets_markets
from utils.db_utils import PostgresHandler

db = PostgresHandler()

def extract_all_data():
    """Executa todo o processo de extração usando os scripts existentes"""
    try:
        print("Iniciando extração de dados da API capcoin")
        
        # Extrai e carrega assets
        print("Extraindo assets")
        df_assets = get_top_assets()
        if df_assets is not None:
            db.insert_dataframe(df_assets, table_name="stg_assets")
            
        # Extrai e carrega exchanges
        print("Extraindo exchanges")
        df_exchanges = get_exchanges()
        if df_exchanges is not None:
            db.insert_dataframe(df_exchanges, table_name="stg_exchanges")
            
        # Extrai e carrega asset markets
        print("Extraindo asset markets")
        df_markets = get_all_assets_markets()
        if df_markets is not None:
            db.insert_dataframe(df_markets, table_name="stg_assets_markets")
            
        print("Extração concluída com sucesso!")
        
    except Exception as e:
        print(f"Erro durante a extração: {e}")
        raise e

if __name__ == "__main__":
    extract_all_data()