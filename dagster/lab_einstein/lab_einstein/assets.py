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
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
EINSTEIN_FILES_FOLDER = ROOT_PATH / "data" / "einstein"
EINSTEIN_FILES_EXTENSION = '.xlsx'

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
def einstein_raw(context):
    """
    Read all excel files from data/einstein folder and save to db
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    einstein_files = [file for file in os.listdir(EINSTEIN_FILES_FOLDER) if file.endswith(EINSTEIN_FILES_EXTENSION)]
    assert len(einstein_files) > 0, f"No files found in the folder {EINSTEIN_FILES_FOLDER} with extension {EINSTEIN_FILES_EXTENSION}"

    einstein_df = pd.read_excel(EINSTEIN_FILES_FOLDER / einstein_files[0], dtype = str, sheet_name='itps_dengue')
    einstein_df['file_name'] = einstein_files[0]
    context.log.info(f"Reading file {einstein_files[0]}")

    # Get only the date on 'dh_coleta' column and format it to 'dd/mm/yyyy' 
    try: # Try to get the date in the format 'dd/mm/yyyy'
        einstein_df['dh_coleta'] = pd.to_datetime(einstein_df['dh_coleta'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')
    except: # If it's not in the format 'dd/mm/yyyy', try to get the date in the format 'yyyy-mm-dd'
        einstein_df['dh_coleta'] = pd.to_datetime(einstein_df['dh_coleta'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
        
    # Save to db
    einstein_df.to_sql('einstein_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    n_rows = einstein_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Einstein Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='einstein',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "einstein_06_final")]
)
def einstein_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'einstein_raw'
    files_in_folder = [file for file in os.listdir(EINSTEIN_FILES_FOLDER) if file.endswith(EINSTEIN_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM arboviroses.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = EINSTEIN_FILES_FOLDER / "_out"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            shutil.move(EINSTEIN_FILES_FOLDER / used_file, path_to_move / used_file)
    
    # Log the unmoved files
    files_in_folder = os.listdir(EINSTEIN_FILES_FOLDER)
    context.log.info(f"Files that were not moved: {files_in_folder}")

einstein_all_assets_job = define_asset_job(name="einstein_all_assets_job")

@sensor(
    job=einstein_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_einstein_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the einstein folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the einstein folder
    files = os.listdir(EINSTEIN_FILES_FOLDER)
    valid_files = [file for file in files if file.endswith(EINSTEIN_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'einstein_all_assets_job'
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
        yield SkipReason(f"There are files in the einstein folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
einstein_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[einstein_all_assets_job],
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