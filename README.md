# Crypto Market Analysis

## Sobre o Projeto
Pipeline ETL para análise do mercado de criptomoedas usando dados da CoinCap API. O projeto coleta dados sobre criptomoedas, exchanges e mercados, transformando-os em um modelo dimensional para análise.

### Arquitetura
- **Staging Layer**: Dados brutos da API
- **Presentation Layer**: Modelo dimensional (Star Schema)
- **Containerização**: PostgreSQL em Docker
- **Monitoramento**: Controle de chamadas da API

### Modelo de Dados
1. **Dimensões**:
   - `dim_assets`: Informações sobre criptomoedas
   - `dim_exchanges`: Informações sobre exchanges
   - `dim_trading_pairs`: Pares de trading disponíveis

2. **Fatos**:
   - `fact_asset_metrics`: Métricas diárias de criptomoedas
   - `fact_exchange_metrics`: Métricas diárias de exchanges
   - `fact_market_metrics`: Métricas de mercado por par

## Setup do Ambiente

### Pré-requisitos
- Python 3.11+
- Docker e Docker Compose
- PostgreSQL
- Chave de API do CoinCap

### Instalação

1. **Clone o repositório**
```bash
git clone https://github.com/seu-usuario/crypto-market-analysis.git
cd crypto-market-analysis
```

2. **Crie e ative o ambiente virtual**
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

3. **Instale as dependências**
```bash
pip install -r requirements.txt
```

4. **Configure as variáveis de ambiente**
Crie um arquivo `.env` com:
```env
COINCAP_API_KEY=sua_chave_api
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=crypto_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/crypto_db
```

5. **Inicie o banco de dados**
```bash
docker-compose up -d
```

6. **Initialize o banco de dados**
```bash
python initialize_db.py
```

### Executando o Pipeline
```bash
python main.py
```

## Estrutura do Projeto
```
crypto-market-analysis/
├── sql/                    # Scripts SQL
├── src/                    # Código fonte principal
│   ├── extract.py         # Extração de dados
│   └── transform.py       # Transformações
├── staging/               # Scripts de staging
├── utils/                 # Utilitários
├── notebooks/             # Notebook Jupyter para teste da api
├── docker-compose.yml     # Configuração Docker
└── requirements.txt       # Dependências
```

## Observações 
- A API do CoinCap tem limite de 2500 chamadas por mês
- O pipeline está configurado para coletar top 20 criptomoedas
- Índices otimizados para consultas frequentes

## Tecnologias Utilizadas
- Python
- PostgreSQL
- SQLAlchemy
- Pandas
- Docker
