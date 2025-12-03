import logging
import azure.functions as func
from letrus_job import job
from pathlib import Path
import json
from sql_to_supabase import clear_supabase_table, insert_enriched_data

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 6 * * *", arg_name="letrusTimer", run_on_startup=False, use_monitor=False)
def func_letrus(letrusTimer: func.TimerRequest) -> None:
    job.process_aux()
    job.process_silver()

@app.timer_trigger(schedule="0 0 */2 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def func_sql_to_supabase(myTimer: func.TimerRequest) -> None:
    logging.info("⏰ Iniciando execução diária de múltiplas migrações")

    base_path = Path(__file__).parent
    config_path = base_path / "sql_to_supabase" /"config" / "mappings.json"

    with open(config_path, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    for item in mappings:
        table_name = item["table_name"]
        query_file = base_path / item["query_file"]

        with open(query_file, "r", encoding="utf-8") as q:
            query = q.read()

        logging.info(f"🚀 Processando tabela '{table_name}'")

        try:
            # Limpa tabela
            clear_supabase_table(table_name)

            # Executa query e envia dados
            insert_enriched_data(table_name, query)

            logging.info(f"✅ Concluído: {table_name}")

        except Exception as e:
            logging.error(f"❌ Erro na tabela '{table_name}': {str(e)}")