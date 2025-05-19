from sqlalchemy import create_engine
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class PostgresHandler:
    """Classe para manipulação de dados no PostgreSQL"""

    def __init__(self, db_url=None):
        """
        Inicializa a classe PostgresHandler.

        Parâmetros:
            db_url (str, opcional): URL de conexão com o banco de dados
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")
        if not self.db_url:
            raise ValueError("DATABASE_URL não foi fornecida nem definida no .env")
        self.engine = create_engine(self.db_url)

    def _normalize_column_names(self, df):
        """Normaliza nomes das colunas do DataFrame"""
        df.columns = df.columns.str.lower()
        return df

    def insert_dataframe(self, df, table_name, schema="staging_layer"):
        """Insere DataFrame em uma tabela"""
        try:
            df = self._normalize_column_names(df)
            
            with self.engine.begin() as connection:
                df.to_sql(
                    name=table_name,
                    con=connection,
                    schema=schema,
                    if_exists='append',
                    index=False
                )
            print(f"Dados inseridos em {schema}.{table_name}")
        except Exception as e:
            print(f"Falha ao inserir dados: {e}")

    def read_query(self, query):
        """Executa uma query e retorna os resultados como DataFrame"""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao executar query: {e}")
            return None
        
    def get_distinct_values(self, column, table_name, schema="staging_layer", where=None):
        """Busca valores distintos de uma coluna"""
        query = f"SELECT DISTINCT {column} FROM {schema}.{table_name}"
        if where:
            query += f" WHERE {where}"
        return self.read_query(query)