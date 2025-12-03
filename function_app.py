import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyodbc, os
from io import BytesIO
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="letrus")
@app.timer_trigger(schedule="0 0 6 * * *", arg_name="letrusTimer", run_on_startup=False, use_monitor=False)
def func_letrus(letrusTimer: func.TimerRequest) -> None:
    

    # ==========================
    # CONFIGURAÇÕES
    # ==========================

    # Container e arquivo XLSX
    BLOB_CONN_STR = os.getenv("BLOB_CONNECTION_STRING")
    CONTAINER_NAME = "01-raw"
    CONTAINER_LETRUS = CONTAINER_NAME
    BLOB_PATH = "letrus/detalhamento_notas_por_redacao_enem_2025-11-25.xlsx"

    # Tabela destino
    SQL_TABLE = "dbo.ismart_letrus_cj"

    logging.info("🟦 Azure Function iniciada...")

    # ======================================================
    # 1. BAIXAR ARQUIVO DO BLOB STORAGE
    # ======================================================
    try:
        logging.info("🔵 Conectando ao Blob Storage...")
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        container_client = blob_service.get_container_client(CONTAINER_LETRUS)
        blob_client = container_client.get_blob_client(BLOB_PATH)

        logging.info(f"📥 Baixando arquivo: {BLOB_PATH} ...")

    except Exception as e:
        logging.error(f"❌ Erro ao ler Blob: {e}")
        return

    # ======================================================
    # 2. LER XLSX COM PANDAS
    # ======================================================
    try:
        logging.info("📗 Lendo XLSX com pandas...")
         # Lê o arquivo em BYTES (CORRETO)
        file_bytes = blob_client.download_blob().readall()

        # Verifique se está correto (opcional)
        print("Tipo:", type(file_bytes))
        print("Tamanho:", len(file_bytes))

        # Lê o Excel com pandas usando BytesIO (CORRETO)
        df = pd.read_excel(BytesIO(file_bytes))
        
        df.columns = (
            df.columns.str.strip()                      # remove espaços no início/fim
                .str.normalize('NFKD')                  # remove acentos
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace(' ', '_')                  # troca espaço por _
                .str.replace(r'[^A-Za-z0-9_]', '', regex=True) # tira caracteres estranhos
        )


        # Remove colunas completamente vazias
        df = df.dropna(axis=1, how="all")

        # Remove linhas completamente vazias
        df = df.dropna(how="all")

        # Converte tudo para string
        df = df.astype(str)

        # Replace de valores nulos
        df = df.replace(["nan", "NaN", "None", ""], None)


        print("Prévia do DataFrame:")
        print(df.head())
        logging.info(f"✔ Arquivo carregado com {len(df)} linhas.")

    except Exception as e:
        logging.error(f"❌ Erro ao ler XLSX: {e}")
        return

    # ======================================================
    # 3. CONECTAR AO SQL SERVER
    # ======================================================
    try:
        logging.info("🟣 Conectando ao SQL Server...")

        server = 'ismart-sql-server.database.windows.net'
        database = 'dev-ismart-sql-db'
        username = 'ismart'
        password = 'th!juyep8iFr'
        driver = "ODBC Driver 18 for SQL Server"

        # Build connection string with SQL authentication
        conn_str = (
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

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        logging.info("✔ Conexão com SQL estabelecida.")

    except Exception as e:
        logging.error(f"❌ Erro ao conectar ao SQL Server: {e}")
        return

    # ======================================================
    # 4. INSERIR OS DADOS NO SQL SERVER
    # ======================================================
    try:
        logging.info("🟩 Inserindo dados no SQL Server...")

        # ======================================================
        # CRIAÇÃO AUTOMÁTICA DA TABELA SE NÃO EXISTIR
        # ======================================================
        logging.info("🟧 Verificando se a tabela existe...")

        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='{SQL_TABLE.split('.')[-1]}' AND xtype='U'
            )
            SELECT 0 ELSE SELECT 1
        """)

        exists = cursor.fetchone()[0]

        if exists == 0:
            logging.info("🆕 Criando tabela automaticamente...")

            # Gera o DDL (schema SQL) baseado no DataFrame
            sql_cols = []
            for col, dtype in df.dtypes.items():
                # if "int" in str(dtype):
                #     sql_type = "INT"
                # elif "float" in str(dtype) or "double" in str(dtype):
                #     sql_type = "FLOAT"
                # elif "datetime" in str(dtype):
                #     sql_type = "DATETIME"
                # else:
                sql_type = "NVARCHAR(255)"  # fallback seguro

                sql_cols.append(f"[{col}] {sql_type}")

            create_sql = f"""
                CREATE TABLE {SQL_TABLE} (
                    {",".join(sql_cols)}
                )
            """

            cursor.execute(create_sql)
            conn.commit()

            logging.info("✔ Tabela criada com sucesso!")

    
        cols = ",".join(df.columns)
        placeholders = ",".join(["?"] * len(df.columns))

        insert_sql = f"INSERT INTO {SQL_TABLE} ({cols}) VALUES ({placeholders})"
        
        batch_size = 1000
        records = df.astype(str).values.tolist()

        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            print(len(batch))
            cursor.executemany(insert_sql, batch)

        conn.commit()

        logging.info("✔ Todos os dados foram inseridos com sucesso!")

    except Exception as e:
        logging.error(f"❌ Erro ao inserir dados no SQL Server: {e}")
    finally:
        cursor.close()
        conn.close()

    logging.info("🏁 Azure Function finalizada.")
