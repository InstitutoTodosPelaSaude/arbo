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
from textwrap import dedent
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
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

INFODENGUE_FILES_FOLDER = "/data/arbo/data/InfoDengue/"
INFODENGUE_FILES_EXTENSION = '.csv'

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
def infodengue_raw(context):
    """
    Read all excel files from data/InfoDengue folder and save to db
    """
    file_system = FileSystem(root_path=INFODENGUE_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    infodengue_files = file_system.list_files_in_relative_path("")
    infodengue_files = [file for file in infodengue_files if file] # remove NONE
    infodengue_files = [file for file in infodengue_files if file.endswith(INFODENGUE_FILES_EXTENSION)]
    assert len(infodengue_files) > 0, f"No files found in the folder {INFODENGUE_FILES_FOLDER} with extension {INFODENGUE_FILES_EXTENSION}"

    file_to_get = infodengue_files[0].split("/")[-1] # Get the file name
    infodengue_df = pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get))
    infodengue_df['file_name'] = infodengue_files[0]
    context.log.info(f"Reading file {infodengue_files[0]}")
        
    # Save to db
    infodengue_df.to_sql('infodengue_raw', engine, schema='arboviroses', if_exists='replace', chunksize=10000, index=False)
    engine.dispose()

    n_rows = infodengue_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # infodengue Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='infodengue',
    dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

infodengue_all_assets_job = define_asset_job(name="infodengue_all_assets_job")

@sensor(
    job=infodengue_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=15 # 15 seconds
)
def new_infodengue_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the infodengue folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the infodengue folder
    file_system = FileSystem(root_path=INFODENGUE_FILES_FOLDER)
    files = file_system.list_files_in_relative_path("")
    valid_files = [file for file in files if file.endswith(INFODENGUE_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'infodengue_all_assets_job'
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
        yield SkipReason(f"There are files in the infodengue folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
infodengue_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[infodengue_all_assets_job],
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