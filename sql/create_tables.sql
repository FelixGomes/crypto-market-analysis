CREATE TABLE IF NOT EXISTS staging_layer.stg_assets (
    id text,
    rank integer,
    symbol text,
    name text,
    supply numeric,
    maxsupply numeric,
    marketcapusd numeric,
    volumeusd24hr numeric,
    priceusd numeric,
    changepercent24hr numeric,
    vwap24hr numeric,
    explorer text,
    ingestion_timestamp timestamp
);

CREATE TABLE IF NOT EXISTS staging_layer.stg_asset_markets (
    asset_id text,
    exchangeid text,
    baseid text,
    quoteid text,
    basesymbol text,
    quotesymbol text,
    volumeusd24hr numeric,
    priceusd numeric,
    volumepercent numeric,
    ingestion_timestamp timestamp
);

CREATE TABLE IF NOT EXISTS staging_layer.stg_exchanges (
    id text,
    name text,
    rank text,
    percent_total_volume numeric,
    volume_usd numeric,
    trading_pairs text,
    exchange_url text,
    ingestion_timestamp timestamp
);

-- Dimensões
CREATE TABLE IF NOT EXISTS presentation_layer.dim_assets (
    asset_id text,
    symbol text,
    name text,
    explorer text,
    effective_date date,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS presentation_layer.dim_exchanges (
    exchange_id text,
    name text,
    exchange_url text,
    effective_date date,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS presentation_layer.dim_trading_pairs (
    base_id text,
    quote_id text,
    base_symbol text,
    quote_symbol text,
    effective_date date,
    updated_at timestamp
);

-- Fatos
CREATE TABLE IF NOT EXISTS presentation_layer.fact_asset_metrics (
    date_id date,
    asset_id text,
    rank integer,
    price_usd numeric,
    volume_usd_24hr numeric,
    market_cap_usd numeric,
    change_percent_24hr numeric,
    vwap_24hr numeric,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS presentation_layer.fact_exchange_metrics (
    date_id date,
    exchange_id text,
    volume_usd numeric,
    trading_pairs integer,
    percent_total_volume numeric,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS presentation_layer.fact_market_metrics (
    date_id date,
    asset_id text,
    exchange_id text,
    base_id text,
    quote_id text,
    price_usd numeric,
    volume_usd_24hr numeric,
    volume_percent numeric,
    updated_at timestamp
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_asset_metrics_date 
ON presentation_layer.fact_asset_metrics(date_id);

CREATE INDEX IF NOT EXISTS idx_exchange_metrics_date 
ON presentation_layer.fact_exchange_metrics(date_id);

CREATE INDEX IF NOT EXISTS idx_market_metrics_date 
ON presentation_layer.fact_market_metrics(date_id);

CREATE INDEX IF NOT EXISTS idx_market_metrics_asset 
ON presentation_layer.fact_market_metrics(asset_id);

CREATE INDEX IF NOT EXISTS idx_market_metrics_exchange 
ON presentation_layer.fact_market_metrics(exchange_id);