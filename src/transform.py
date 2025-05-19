import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))
from utils.db_utils import PostgresHandler

db = PostgresHandler()

def transform_dim_assets():
    """Transforma dados para dimensão de assets"""
    query = text("""
    TRUNCATE TABLE presentation_layer.dim_assets;
    
    INSERT INTO presentation_layer.dim_assets
    SELECT DISTINCT
        id as asset_id,
        symbol,
        name,
        explorer,
        CURRENT_DATE as effective_date,
        CURRENT_TIMESTAMP as updated_at
    FROM staging_layer.stg_assets;
    """)
    
    try:
        with db.engine.begin() as conn:
            conn.execute(query)
            print("Dimensão assets transformada com sucesso")
    except Exception as e:
        print(f"Erro ao transformar dimensão assets: {e}")
        raise e

def transform_dim_exchanges():
    query = text("""
    TRUNCATE TABLE presentation_layer.dim_exchanges;
    
    INSERT INTO presentation_layer.dim_exchanges
    SELECT DISTINCT
        id as exchange_id,
        name,
        exchange_url,
        CURRENT_DATE as effective_date,
        CURRENT_TIMESTAMP as updated_at
    FROM staging_layer.stg_exchanges;
    """)
    
    try:
        with db.engine.begin() as conn:
            conn.execute(query)
            print("Dimensão exchanges transformada com sucesso")
    except Exception as e:
        print(f"Erro ao transformar dimensão exchanges: {e}")
        raise e

def transform_dim_trading_pairs():
    query = text("""
    TRUNCATE TABLE presentation_layer.dim_trading_pairs;
    
    INSERT INTO presentation_layer.dim_trading_pairs
    SELECT DISTINCT
        baseid as base_id,
        quoteid as quote_id,
        basesymbol as base_symbol,
        quotesymbol as quote_symbol,
        CURRENT_DATE as effective_date,
        CURRENT_TIMESTAMP as updated_at
    FROM staging_layer.stg_asset_markets;
    """)
    
    try:
        with db.engine.begin() as conn:
            conn.execute(query)
            print("Dimensão trading pairs transformada com sucesso")
    except Exception as e:
        print(f"Erro ao transformar dimensão trading pairs: {e}")
        raise e

def transform_fact_asset_metrics():
    query = text("""
    INSERT INTO presentation_layer.fact_asset_metrics
    SELECT
        CURRENT_DATE as date_id,
        id as asset_id,
        rank::integer,
        priceusd::numeric as price_usd,
        volumeusd24hr::numeric as volume_usd_24hr,
        marketcapusd::numeric as market_cap_usd,
        changepercent24hr::numeric as change_percent_24hr,
        vwap24hr::numeric as vwap_24hr,
        CURRENT_TIMESTAMP as updated_at
    FROM staging_layer.stg_assets;
    """)
    
    try:
        with db.engine.begin() as conn:
            conn.execute(query)
            print("Fato asset metrics transformado com sucesso")
    except Exception as e:
        print(f"Erro ao transformar fato asset metrics: {e}")
        raise e

def transform_fact_exchange_metrics():
    query = text("""
    INSERT INTO presentation_layer.fact_exchange_metrics
    SELECT
        CURRENT_DATE as date_id,
        id as exchange_id,
        volume_usd::numeric,
        trading_pairs::integer,
        percent_total_volume::numeric,
        CURRENT_TIMESTAMP as updated_at
    FROM staging_layer.stg_exchanges;
    """)
    
    try:
        with db.engine.begin() as conn:
            conn.execute(query)
            print("Fato exchange metrics transformado com sucesso")
    except Exception as e:
        print(f"Erro ao transformar fato exchange metrics: {e}")
        raise e

def transform_fact_market_metrics():
    query = text("""
    INSERT INTO presentation_layer.fact_market_metrics
    SELECT
        CURRENT_DATE as date_id,
        asset_id,
        exchangeid as exchange_id,
        baseid as base_id,
        quoteid as quote_id,
        basesymbol as base_symbol,    -- Adicionado
        quotesymbol as quote_symbol,  -- Adicionado
        priceusd::numeric as price_usd,
        volumeusd24hr::numeric as volume_usd_24hr,
        volumepercent::numeric as volume_percent,
        CURRENT_TIMESTAMP as updated_at
    FROM staging_layer.stg_asset_markets;
    """)
    
    try:
        with db.engine.begin() as conn:
            conn.execute(query)
            print("Fato market metrics transformado com sucesso")
    except Exception as e:
        print(f"Erro ao transformar fato market metrics: {e}")
        raise e

def transform_all():
    """Executa todas as transformações"""
    try:
        print("Iniciando transformações")
        
        # Dimensões (overwrite)
        transform_dim_assets()
        transform_dim_exchanges()
        
        # Fatos (append)
        transform_fact_asset_metrics()
        transform_fact_exchange_metrics()
        transform_fact_market_metrics()
        
        print("Transformações concluídas com sucesso!")
    except Exception as e:
        print(f"Erro durante as transformações: {e}")
        raise e

if __name__ == "__main__":
    transform_all()