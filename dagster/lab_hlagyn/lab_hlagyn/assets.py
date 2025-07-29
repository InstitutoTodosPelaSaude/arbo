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
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil
import re

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

HLAGYN_FILES_FOLDER = "/data/arbo/data/hlagyn/"
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
    file_system = FileSystem(root_path=HLAGYN_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    hlagyn_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HLAGYN_FILES_EXTENSION)]
    assert len(hlagyn_files) > 0, f"No files found in the folder {HLAGYN_FILES_FOLDER} with extension {HLAGYN_FILES_EXTENSION}"

    file_to_get = hlagyn_files[0].split("/")[-1] # Get the file name
    hlagyn_df = pd.read_excel(file_system.get_file_content_as_io_bytes(file_to_get), dtype = str)
    if 'Unnamed: 0' in hlagyn_df.columns: 
        # Some files have an empty row in the beginning
        hlagyn_df = pd.read_excel(file_system.get_file_content_as_io_bytes(file_to_get), skiprows=1, dtype = str)
    hlagyn_df['file_name'] = hlagyn_files[0]
    context.log.info(f"Reading file {hlagyn_files[0]}")

    # Normalize 'Cidade' and 'UF' columns when they come with another column name
    if 'Cidade' not in hlagyn_df.columns:
        hlagyn_df.rename(columns={
                'Cidade (Inst Saúde)': 'Cidade',
                'Cidade  (Inst Saúde)': 'Cidade'
            }, inplace=True)

    if 'UF' not in hlagyn_df.columns:
        hlagyn_df.rename(columns={
                'UF (Inst Saúde)': 'UF',
                'UF  (Inst Saúde)': 'UF'
            }, inplace=True)
        
    # Sometimes the result columns like 'CT_I', 'CT_N', 'CT_ORF1AB', etc. appear as 'Resultado' in the files.
    # We need to rename them back to 'CT_I', 'CT_N', 'CT_ORF1AB', etc., using the values from the columns.
    # For example: If all values start with 'CT_I:', we rename the column to 'CT_I'.
    
    # Select all columns with the name "Resultado", "Resultado.1", etc.
    resultado_cols = [col for col in hlagyn_df.columns if col.startswith("Resultado")]

    for col in resultado_cols:
        # Consider both NaN and empty or whitespace-only strings as empty
        col_sem_na = hlagyn_df[col].dropna()
        col_sem_na = col_sem_na[col_sem_na.astype(str).str.strip() != ""]
        if col_sem_na.empty:
            hlagyn_df.drop(columns=[col], inplace=True)
            continue  # If the column is empty, skip to the next iteration

        # Get the first non-null value to extract the prefix (e.g., "CT_I:")
        first_value = hlagyn_df[col].dropna().astype(str).iloc[0]
        match = re.match(r'^([\w\d]+):\s*(.*)', first_value)
        if match:
            prefix = match.group(1)  # Example: "CT_I"
            # 3. Rename the column
            hlagyn_df.rename(columns={col: prefix}, inplace=True)
            # 4. Remove the prefix from the column values
            hlagyn_df[prefix] = (
                hlagyn_df[prefix]
                .astype(str)
                .str.replace(r'^[\w\d]+:\s*', '', regex=True)
                .replace({'nan': '', 'None': ''})  # Fix "nan" and "None" values to empty string
                .apply(lambda x: x.strip() if isinstance(x, str) else x)  # Remove extra spaces
            )

    # After processing, check if only one 'Resultado' column remains
    resultado_restantes = [col for col in hlagyn_df.columns if col.startswith("Resultado")]
    if len(resultado_restantes) == 1:
        col_antiga = resultado_restantes[0]
        hlagyn_df.rename(columns={col_antiga: "Resultado"}, inplace=True)

    # Map columns when identifying a ARBOVIRUS file
    if 'VIRUS_ZIKA' in hlagyn_df.columns:
        # Rename the columns to match the expected names
        hlagyn_df.rename(columns={
            'VIRUS_ZIKA': 'Zika vírus',
            'VIRUS_DENGUE': 'Dengue vírus',
            'VIRUS_CHIKU': 'Chikungunya vírus',
        }, inplace=True)
        
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
    file_system = FileSystem(root_path=HLAGYN_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HLAGYN_FILES_EXTENSION)]

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
    file_system = FileSystem(root_path=HLAGYN_FILES_FOLDER)
    files = file_system.list_files_in_relative_path("")
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