from dagster import (
    AssetExecutionContext, 
    asset,
    sensor,
    define_asset_job,
    RunRequest,
    SkipReason, 
    SensorEvaluationContext,
    DefaultSensorStatus
)
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    get_asset_key_for_model
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES, DagsterRunStatus
from dagster_slack import make_slack_on_run_failure_sensor
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
HLAGYN_FILES_FOLDER = ROOT_PATH / "data" / "hlagyn"
HLAGYN_FILES_EXTENSION = '.xlsx'

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@asset(compute_kind="python")
def hlagyn_raw(context):
    """
    Table with the raw data from hlagyn
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    hlagyn_files = [file for file in os.listdir(HLAGYN_FILES_FOLDER) if file.endswith(HLAGYN_FILES_EXTENSION)]
    assert len(hlagyn_files) > 0, f"No files found in the folder {HLAGYN_FILES_FOLDER} with extension {HLAGYN_FILES_EXTENSION}"

    hlagyn_df = pd.read_excel(HLAGYN_FILES_FOLDER / hlagyn_files[0], dtype = str)
    if 'Unnamed: 0' in hlagyn_df.columns: 
        # Some files have an empty row in the beginning
        hlagyn_df = pd.read_excel(HLAGYN_FILES_FOLDER / hlagyn_files[0], skiprows=1, dtype = str)
    hlagyn_df['file_name'] = hlagyn_files[0]
    context.log.info(f"Reading file {hlagyn_files[0]}")
        
    # Save to db
    hlagyn_df.to_sql('hlagyn_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': hlagyn_df.shape[0]})

@dbt_assets(manifest=dbt_manifest_path, 
            select='hlagyn',
            dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "hlagyn_05_final")]
)
def hlagyn_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'hlagyn_raw'
    files_in_folder = [file for file in os.listdir(HLAGYN_FILES_FOLDER) if file.endswith(HLAGYN_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM arboviroses.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = HLAGYN_FILES_FOLDER / "_out"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            shutil.move(HLAGYN_FILES_FOLDER / used_file, path_to_move / used_file)
    
    # Log the unmoved files
    files_in_folder = os.listdir(HLAGYN_FILES_FOLDER)
    context.log.info(f"Files that were not moved: {files_in_folder}")

hlagyn_all_assets_job = define_asset_job(name="hlagyn_all_assets_job")

@sensor(
    job=hlagyn_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_hlagyn_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the hlagyn folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the hlagyn folder
    files = os.listdir(HLAGYN_FILES_FOLDER)
    valid_files = [file for file in files if file.endswith(HLAGYN_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'hlagyn_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # If there are no runs running, run the job
    if last_run_status in FINISHED_STATUSES or last_run_status is None:
        # Do not run if the last status is an error
        if last_run_status == DagsterRunStatus.FAILURE:
            return SkipReason(f"Last run status is an error status: {last_run_status}")

        yield RunRequest()
    else:
        yield SkipReason(f"There are files in the hlagyn folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
hlagyn_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[hlagyn_all_assets_job],
    slack_token=DAGSTER_SLACK_BOT_TOKEN,
    channel=DAGSTER_SLACK_BOT_CHANNEL,
    default_status=DefaultSensorStatus.RUNNING,
    text_fn = lambda context: f"LAB JOB FAILED: {context.failure_event.message}"
)