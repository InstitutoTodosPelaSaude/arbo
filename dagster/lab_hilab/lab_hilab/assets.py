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
from dagster.core.storage.dagster_run import FINISHED_STATUSES
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
HILAB_FILES_FOLDER = ROOT_PATH / "data" / "hilab"
HILAB_FILES_EXTENSION = '.csv'

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@asset(compute_kind="python")
def hilab_raw(context):
    """
    Table with the raw data from Hilab
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    hilab_df = pd.DataFrame()
    for file in os.listdir(HILAB_FILES_FOLDER):
        if not file.endswith(HILAB_FILES_EXTENSION):
            continue

        df = pd.read_csv(HILAB_FILES_FOLDER / file)
        df['file_name'] = file
        hilab_df = pd.concat([hilab_df, df], ignore_index=True)
        
    # Save to db
    hilab_df.to_sql('hilab_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': hilab_df.shape[0]})

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='hilab',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "hilab_05_final")]
)
def hilab_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'hilab_raw'
    files_in_folder = [file for file in os.listdir(HILAB_FILES_FOLDER) if file.endswith(HILAB_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM arboviroses.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = HILAB_FILES_FOLDER / "_out"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            shutil.move(HILAB_FILES_FOLDER / used_file, path_to_move / used_file)
    
    # Log the unmoved files
    files_in_folder = os.listdir(HILAB_FILES_FOLDER)
    context.log.info(f"Files that were not moved: {files_in_folder}")

hilab_all_assets_job = define_asset_job(name="hilab_all_assets_job")

@sensor(
    job=hilab_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_hilab_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the hilab folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the hilab folder
    files = os.listdir(HILAB_FILES_FOLDER)
    valid_files = [file for file in files if file.endswith(HILAB_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'hilab_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # If there are no runs running, run the job
    if last_run_status in FINISHED_STATUSES or last_run_status is None:
        yield RunRequest()
    else:
        yield SkipReason(f"There are files in the hilab folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")
