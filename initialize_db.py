from pathlib import Path
from sqlalchemy import text
from utils.db_utils import PostgresHandler

def read_sql_file(file_path):
    """Lê um arquivo SQL e retorna seu conteúdo"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            if not content.strip():
                raise ValueError(f"Arquivo SQL vazio: {file_path}")
            return content
    except Exception as e:
        print(f"Erro ao ler arquivo {file_path}: {e}")
        raise e

def initialize_database():
    """Inicializa o banco de dados com schemas e tabelas"""
    db = PostgresHandler()
    sql_dir = Path(__file__).parent / 'sql'
    sql_files = ['create_schemas.sql', 'create_tables.sql']
    
    try:
        with db.engine.begin() as conn:
            for sql_file in sql_files:
                print(f"Executando {sql_file}...")
                sql_path = sql_dir / sql_file
                sql_content = read_sql_file(sql_path)
                
                # Executa cada comando SQL separadamente
                for command in sql_content.split(';'):
                    if command.strip():
                        conn.execute(text(command))
                
                print(f"{sql_file} executado com sucesso")
        print("Banco de dados inicializado com sucesso!")
        
    except Exception as e:
        print(f"Erro ao inicializar banco de dados: {e}")
        raise e

if __name__ == "__main__":
    initialize_database()