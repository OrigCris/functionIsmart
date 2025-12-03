import logging
import azure.functions as func
from letrus_job import job

app = func.FunctionApp()

@app.function_name(name="letrus")
@app.timer_trigger(schedule="0 0 6 * * *", arg_name="letrusTimer", run_on_startup=False, use_monitor=False)
def func_letrus(letrusTimer: func.TimerRequest) -> None:
    job.process_aux()
    job.process_silver()