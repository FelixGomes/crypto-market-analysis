import sys
from pathlib import Path
from datetime import datetime
from initialize_db import initialize_database

from src.extract import extract_all_data
from src.transform import transform_all

def run_etl():
    """Executa o pipeline ETL completo"""
    try:
        # Extração (Staging Layer)
        print("Iniciando extração de dados da CoinCap API")
        extract_all_data()
        print("Extração concluída com sucesso")
        
        # Transformação (Presentation Layer)
        print("Iniciando transformações e modelagem star schema")
        transform_all()
        print("Transformações concluídas com sucesso")
        
    except Exception as e:
        print(f"Erro durante o ETL: {e}")
        raise e

def main():
    """Executa todo o pipeline ETL"""
    start_time = datetime.now()
    print("Iniciando pipeline ETL...")
    
    try:
        # Inicialização do banco
        print("Inicializando banco de dados...")
        initialize_database()

        # Execução do ETL
        run_etl()
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\nPipeline ETL concluído com sucesso!")
        print(f"Tempo total de execução: {duration}")
        
    except Exception as e:
        print(f"\nErro na execução do pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()