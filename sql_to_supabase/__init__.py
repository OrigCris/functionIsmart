import json
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging, os, pyodbc, requests
from supabase import create_client, Client
from datetime import date, datetime

KEY_VAULT_NAME = os.environ["KEY_VAULT_NAME"]


def serialize_value(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()  # exemplo: '2025-11-12' ou '2025-11-12T06:00:00'
    return v

def get_secret_from_keyvault(secret_name: str) -> str:
    """Get secret from Azure Key Vault"""
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=KEY_VAULT_NAME, credential=credential)
        return client.get_secret(secret_name).value
    except Exception as e:
        logging.error(f"Error retrieving secret from Key Vault: {str(e)}")
        raise

def get_supabase_client() -> Client:
    """Get authenticated Supabase client"""
    url = 'https://fqtdvwmkmqgeiyudicur.supabase.co'
    key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZxdGR2d21rbXFnZWl5dWRpY3VyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY4NTY3NDYsImV4cCI6MjA3MjQzMjc0Nn0.mzhBcfxCTLgWa463IDDx7vWZjyta7ztfYZc4GR9arG4'
    supabase = create_client(url, key)
    return supabase


def get_database_connection():
    """Get database connection using SQL Server authentication"""
    logging.info("Attempting to establish database connection")

    try:
        # Get database credentials from Key Vault
        server = 'ismart-sql-server.database.windows.net'
        database = 'dev-ismart-sql-db'
        username = 'ismart'
        password = 'th!juyep8iFr'
        driver = "ODBC Driver 18 for SQL Server"

        # Build connection string with SQL authentication
        CONNECTION_STRING = (
            f'Driver={{{driver}}};'
            f'Server={server};'
            f'Database={database};'
            f'UID={username};'
            f'PWD={password};'
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=30;'
            'Login Timeout=15;'
        )

        logging.info(f"Using connection string: {CONNECTION_STRING}")

        # Establish connection
        conn = pyodbc.connect(CONNECTION_STRING)

        logging.info("Database connection established successfully")
        return conn
    except Exception as e:
        logging.error(f"Failed to establish database connection: {str(e)}")
        raise

def send_to_supabase(records: list, table_name: str):
    """Insert or upsert records into Supabase"""
    if not records:
        logging.warning("⚠️ No records to send to Supabase")
        return

    try:
        supabase = get_supabase_client()

        logging.info(f"📤 Sending {len(records)} records to Supabase table '{table_name}'")
        response = supabase.table(table_name).insert(records).execute()

        if response.data:
            logging.info(f"✅ Successfully inserted {len(response.data)} records into {table_name}")
        else:
            logging.warning(f"⚠️ Supabase returned no data in response")

    except Exception as e:
        logging.error(f"❌ Error sending data to Supabase: {str(e)}")
        raise

def clear_supabase_table(table_name: str):
    """Apaga todos os registros da tabela no Supabase de forma genérica e segura."""
    try:
        supabase = get_supabase_client()
        logging.info(f"🧹 Limpando tabela '{table_name}' no Supabase...")

        # Busca uma amostra para descobrir uma coluna válida
        sample = supabase.table(table_name).select("*").limit(1).execute()

        if not sample.data:
            logging.info(f"ℹ️ Tabela '{table_name}' já está vazia.")
            return

        # Pega o nome da primeira coluna
        first_column = list(sample.data[0].keys())[0]
        first_value = sample.data[0][first_column]

        # Decide a forma de filtro de acordo com o tipo do valor
        if isinstance(first_value, (int, float)):
            filter_expr = supabase.table(table_name).delete().gt(first_column, -999999999)
        elif isinstance(first_value, str):
            filter_expr = supabase.table(table_name).delete().neq(first_column, "")
        else:
            filter_expr = supabase.table(table_name).delete().neq(first_column, None)

        # Executa a deleção
        response = filter_expr.execute()

        count = len(response.data) if response.data else 0
        logging.info(f"✅ Tabela '{table_name}' limpa com sucesso ({count} registros removidos).")

    except Exception as e:
        logging.error(f"❌ Erro ao limpar tabela '{table_name}': {str(e)}")
        raise

def insert_enriched_data(table_name: str, query: str) -> None:
    conn = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        
        results = [
            {col: serialize_value(val) for col, val in zip(columns, row)}
            for row in rows
        ]

        logging.info(f"📊 {len(results)} registros obtidos de '{table_name}'")

        send_to_supabase(results, table_name)

    except Exception as e:
        logging.error(f"❌ Erro em '{table_name}': {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


app = func.FunctionApp()

