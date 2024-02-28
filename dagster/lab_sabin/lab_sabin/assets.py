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
import requests
import shutil

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
SABIN_FILES_FOLDER = ROOT_PATH / "data" / "sabin"
SABIN_RAW_FILES_EXTENSION = '.xlsx'
SABIN_CONVERTED_FILES_EXTENSION = '.csv'

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
def sabin_convert_xlsx_to_csv(context):
    for file in os.listdir(SABIN_FILES_FOLDER):
        if not file.endswith(SABIN_RAW_FILES_EXTENSION):
            continue
        
        file_path = SABIN_FILES_FOLDER / file
        response = requests.post(
            "http://xlsx2csv:2140/convert",
            files={"file": open(file_path, "rb")},
        )

        if response.status_code != 200:
            raise Exception(f"Error converting file {file_path}")
        
        with open(file_path.with_suffix(SABIN_CONVERTED_FILES_EXTENSION), 'wb') as f:
            f.write(response.content)

        context.log.info(f"Converted file {file_path}")

        # Move the original file to _out folder
        shutil.move(file_path, SABIN_FILES_FOLDER / '_out' / file)
        context.log.info(f"Moved file {file_path} to _out folder")


@asset(compute_kind="python", deps=[sabin_convert_xlsx_to_csv])
def sabin_raw(context):
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files to get the columns
    sabin_files = [file for file in os.listdir(SABIN_FILES_FOLDER) if file.endswith(SABIN_CONVERTED_FILES_EXTENSION)]
    assert len(sabin_files) > 0, f"No files found in {SABIN_FILES_FOLDER} with extension {SABIN_CONVERTED_FILES_EXTENSION}"

    sabin_df = pd.read_csv(SABIN_FILES_FOLDER / sabin_files[0], dtype = str)
    sabin_df['file_name'] = sabin_files[0]
    context.log.info(f"Reading file {sabin_files[0]}")

    # Save to db
    sabin_df.to_sql('sabin_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': sabin_df.shape[0]})

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='sabin',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "sabin_07_final")]
)
def sabin_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'sabin_raw'
    files_in_folder = [file for file in os.listdir(SABIN_FILES_FOLDER) if file.endswith(SABIN_CONVERTED_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM arboviroses.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = SABIN_FILES_FOLDER / "_out"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            shutil.move(SABIN_FILES_FOLDER / used_file, path_to_move / used_file)
    
    # Log the unmoved files
    files_in_folder = os.listdir(SABIN_FILES_FOLDER)
    context.log.info(f"Files that were not moved: {files_in_folder}")

sabin_all_assets_job = define_asset_job(name="sabin_all_assets_job")

@sensor(
    job=sabin_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_sabin_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the sabin folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the sabin folder
    files = os.listdir(SABIN_FILES_FOLDER)
    valid_files = [file for file in files if file.endswith(SABIN_RAW_FILES_EXTENSION) or file.endswith(SABIN_CONVERTED_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'sabin_all_assets_job'
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
        yield SkipReason(f"There are files in the sabin folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
sabin_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[sabin_all_assets_job],
    slack_token=DAGSTER_SLACK_BOT_TOKEN,
    channel=DAGSTER_SLACK_BOT_CHANNEL,
    default_status=DefaultSensorStatus.RUNNING,
    text_fn = lambda context: f"LAB JOB FAILED: {context.failure_event.message}"
)