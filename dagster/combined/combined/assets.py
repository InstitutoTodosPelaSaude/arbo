from dagster import (
    AssetExecutionContext, 
    asset,
    multi_asset_sensor,
    define_asset_job,
    AssetKey,
    RunRequest,
    DefaultSensorStatus,
    SensorEvaluationContext,
    SkipReason
)
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets, 
    get_asset_key_for_model,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES
from dagster_slack import make_slack_on_run_failure_sensor
import pandas as pd
import os
import io
import sys
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
from time import sleep
from io import StringIO, BytesIO
import zipfile
from datetime import datetime, time

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

COMBINED_FILES_FOLDER = "/data/arbo/data/combined/"

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

# Time the sensor will wait checking if another asset started running
TIME_CHECKING_RUNNING_ASSETS = 40 # 45 seconds

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='combined +epiweeks +municipios +age_groups +fix_location +fix_state +macroregions +test_methods',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "combined_05_location")]
)
def export_to_tsv(context):
    """
    Get the final combined data from the database and export to tsv
    """
    # Get the file system
    file_system = FileSystem(root_path=COMBINED_FILES_FOLDER)

    # Export to xlsx
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    cursor = engine.raw_connection().cursor()

    # Export data
    tsv_buffer = StringIO()
    cursor.copy_expert(f'COPY (SELECT * FROM arboviroses."combined_05_location") TO STDOUT WITH CSV DELIMITER E\'\t\' HEADER', tsv_buffer)
    tsv_buffer.seek(0)

    file_system.save_content_in_file('', BytesIO(tsv_buffer.getvalue().encode('utf-8')).read(), 'combined.tsv')

    engine.dispose()

@asset(
    compute_kind="python", 
    deps=[export_to_tsv]
)
def zip_exported_file(context):
    """
    Zip the combined exported file
    """
    file_system = FileSystem(root_path=COMBINED_FILES_FOLDER)

    file_to_zip = file_system.get_file_content_as_io_bytes('combined.tsv')

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer,
                         'w',
                         compression=zipfile.ZIP_DEFLATED,
                         compresslevel=9) as zf:
        zf.writestr('combined.tsv', file_to_zip.getvalue())
    zip_buffer.seek(0)

    file_system.save_content_in_file('', zip_buffer.read(), 'combined.zip')
    

combined_all_assets_job = define_asset_job(name="combined_all_assets_job")

@multi_asset_sensor(
    monitored_assets=[
        AssetKey("einstein_06_final"), 
        AssetKey("hilab_05_final"), 
        AssetKey("hlagyn_05_final"), 
        AssetKey("sabin_07_final"), 
        AssetKey("fleury_06_final"),
        AssetKey("dbmol_final"),
        AssetKey("target_final"),
        AssetKey("hpardini_final")
    ],
    job=combined_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=180 # 3 minutes
)
def run_combined_sensor(context: SensorEvaluationContext):
    # Defining constants for the start and end datetime
    START_DAY = 1  # Tuesday
    START_TIME = time(12, 0)  # 12:00 PM (noon)
    END_DAY = 2  # Wednesday
    END_TIME = time(18, 0)  # 6:00 PM

    # Getting the current date and time
    current_datetime = datetime.now()
    current_weekday = current_datetime.weekday()
    current_time = current_datetime.time()

    # Checking if the current time is within the allowed range (Tuesday noon to Thursday 6:00 PM)
    if current_weekday < START_DAY or current_weekday > END_DAY:
        return SkipReason(f"Execution is not permitted outside of the designated window.")
    elif current_weekday == START_DAY and current_time < START_TIME:
        return SkipReason(f"Execution is not permitted outside of the designated window.")
    elif current_weekday == END_DAY and current_time > END_TIME:
        return SkipReason(f"Execution is not permitted outside of the designated window.")

    # Get the last run status of the job
    job_to_look = 'combined_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # Check if the last run is finished
    if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
        return SkipReason(f"Last run status is {last_run_status}")
    
    # All upstream jobs to check if they are finished
    upstream_jobs = [
        'einstein_all_assets_job', 
        'hilab_all_assets_job', 
        'hlagyn_all_assets_job', 
        'sabin_all_assets_job', 
        'fleury_all_assets_job',
        'dbmol_all_assets_job',
        'target_all_assets_job',
        'hpardini_all_assets_job'
    ]

    # Check if there are new lab assets completed and run combined if it is true
    asset_events = context.latest_materialization_records_by_key()
    if any(asset_events.values()):
        # Check if all upstream jobs are finished (avoid running multiple times)
        for _ in range(0, TIME_CHECKING_RUNNING_ASSETS, 5): # Check every 5 seconds for TIME_CHECKING_RUNNING_ASSETS seconds
            sleep(5)
            for job in upstream_jobs:
                last_run = context.instance.get_runs(
                    filters=RunsFilter(job_name=job)
                )
                last_run_status = None
                if len(last_run) > 0:
                    last_run_status = last_run[0].status
                if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
                    return SkipReason(f"Upstream job {job} is not finished. Last run status is {last_run_status}")

        # If all upstream jobs are finished, return RunRequest
        context.advance_all_cursors()
        return RunRequest()
    
# Failure sensor that sends a message to slack
combined_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[combined_all_assets_job],
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