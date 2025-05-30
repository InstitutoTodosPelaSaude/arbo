from dagster import (
    AssetExecutionContext, 
    asset, 
    sensor,
    define_asset_job,
    RunRequest, 
    SkipReason, 
    SensorEvaluationContext,
    MaterializeResult, 
    MetadataValue,
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
from textwrap import dedent
import pandas as pd
import os
import pathlib
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

HPARDINI_FILES_FOLDER = "/data/arbo/data/hpardini/"
HPARDINI_FILES_EXTENSION = '.csv'

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@asset(compute_kind="python")
def hpardini_raw(context):
    """
    Read excel files from data/hpardini folder and save to db
    """
    file_system = FileSystem(root_path=HPARDINI_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    hpardini_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HPARDINI_FILES_EXTENSION)]
    assert len(hpardini_files) > 0, f"No files found in the folder {HPARDINI_FILES_FOLDER} with extension {HPARDINI_FILES_EXTENSION}"

    # Read the file
    context.log.info(f"Reading file {hpardini_files[0]}")
    file_to_get = hpardini_files[0].split("/")[-1] # Get the file name
    hpardini_df = pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get), dtype = str, sep=';', encoding='latin-1')
    hpardini_df['file_name'] = hpardini_files[0]

    # Change all dates from dd/mm/yyyy to yyyy-mm-dd. But the column values can be in both formats
    date_column = 'DATACOLETA'
    hpardini_df[date_column] = pd.to_datetime(hpardini_df[date_column], errors='coerce').dt.strftime('%Y-%m-%d')

    # Save to db
    hpardini_df.to_sql('hpardini_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    n_rows = hpardini_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # hpardini Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='hpardini',
    dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "hpardini_final")]
)
def hpardini_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'hpardini_raw'
    file_system = FileSystem(root_path=HPARDINI_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HPARDINI_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM arboviroses.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = "_out/"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            file_system.move_file_to_folder("", used_file.split("/")[-1], path_to_move)
    
    # Log the unmoved files
    files_in_folder = file_system.list_files_in_relative_path("")
    context.log.info(f"Files that were not moved: {files_in_folder}")

hpardini_all_assets_job = define_asset_job(name="hpardini_all_assets_job")

@sensor(
    job=hpardini_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_hpardini_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the hpardini folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the hpardini folder
    file_system = FileSystem(root_path=HPARDINI_FILES_FOLDER)
    files = file_system.list_files_in_relative_path("")
    valid_files = [file for file in files if file.endswith(HPARDINI_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'hpardini_all_assets_job'
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
        yield SkipReason(f"There are files in the hpardini folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
hpardini_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[hpardini_all_assets_job],
    slack_token=DAGSTER_SLACK_BOT_TOKEN,
    channel=DAGSTER_SLACK_BOT_CHANNEL,
    default_status=DefaultSensorStatus.RUNNING,
    blocks_fn = lambda context: [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"🔴 ARBO: Job '{context.dagster_run.job_name}' failed",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                            "type": "plain_text",
                            "text": f"{context.failure_event.message}"
                    }
                }
            ]
)