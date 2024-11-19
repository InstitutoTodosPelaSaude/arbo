from dagster import (
    AssetExecutionContext, 
    asset,
    asset_sensor,
    AssetKey,
    define_asset_job,
    DefaultSensorStatus,
    RunRequest,
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
import pathlib
import sys
import io
from sqlalchemy import create_engine
from dotenv import load_dotenv

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

MATRICES_FILES_FOLDER = "/data/arbo/data/matrices/"

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

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='matrices',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_ALL_pos_by_month_agegroups_renamed"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_ARBO_pos_by_month_agegroups_renamed"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_pos_by_month_agegroups_renamed"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_pos_by_epiweek_state"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_ALL_pos_by_month_agegroups"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_posrate_by_epiweek_agegroups"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_posrate_by_epiweek_state_filtered"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_posrate_by_epiweek_state"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_posrate_by_epiweek_year"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_posrate_pos_neg_by_epiweek"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_totaltests_by_epiweek_region"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_pos_direct_cities"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_DENV_pos_direct_states"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_CHIKV_pos_direct_cities"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_CHIKV_pos_direct_states"),
        get_asset_key_for_model([arboviroses_dbt_assets], "01_DENV_line_posrate_direct_week_country_years"),
        get_asset_key_for_model([arboviroses_dbt_assets], "02_DENV_line_bar_posrate_posneg_direct_week_country"),
        get_asset_key_for_model([arboviroses_dbt_assets], "03_DENV_bar_total_direct_weeks_regions"),
        get_asset_key_for_model([arboviroses_dbt_assets], "04_DENV_line_posrate_direct_weeks_states"),
        get_asset_key_for_model([arboviroses_dbt_assets], "06_DENV_heat_posrate_agegroups_week_country"),
        get_asset_key_for_model([arboviroses_dbt_assets], "07_DENV_barH_pos_agegroups_month_country"),
        get_asset_key_for_model([arboviroses_dbt_assets], "08_Arbo_barH_pos_agegroups_month_country"),
        get_asset_key_for_model([arboviroses_dbt_assets], "11_DENV_map_pos_direct_states"),
        get_asset_key_for_model([arboviroses_dbt_assets], "11_DENV_map_pos_direct_cities"),
        get_asset_key_for_model([arboviroses_dbt_assets], "12_CHIKV_map_pos_direct_states"),
        get_asset_key_for_model([arboviroses_dbt_assets], "12_CHIKV_map_pos_direct_cities"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_ALL_count_by_labid_testkit_pathogen_result")
    ]
)
def export_matrices_to_xlsx(context):
    """
    Export all new matrices to XLSX files. The XLSX files are saved to the `matrices` folder.
    """
    
    # Connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # List of matrix tables
    matrix_tables = ['01_DENV_line_posrate_direct_week_country_years', '02_DENV_line_bar_posrate_posneg_direct_week_country',
                     '03_DENV_bar_total_direct_weeks_regions', '04_DENV_line_posrate_direct_weeks_states',
                     '06_DENV_heat_posrate_agegroups_week_country', '07_DENV_barH_pos_agegroups_month_country',
                     '08_Arbo_barH_pos_agegroups_month_country', '11_DENV_map_pos_direct_states', '11_DENV_map_pos_direct_cities', 
                     '12_CHIKV_map_pos_direct_states', '12_CHIKV_map_pos_direct_cities',
                     'matrix_ALL_count_by_labid_testkit_pathogen_result']

    # Get file system
    file_system = FileSystem(root_path=MATRICES_FILES_FOLDER)

    # Delete all the files in the folder to avoid unnecessary files
    for file in file_system.list_files_in_relative_path(""):
        file = file.split("/")[-1] # Get the file name
        deleted = file_system.delete_file(file)

        if not deleted:
            raise Exception(f'Error deleting file {file}')
        context.log.info(f'Deleted {file}')

    # Export each matrix table to a XLSX file
    for table in matrix_tables:
        df = pd.read_sql_query(f'SELECT * FROM arboviroses."{table}"', engine, dtype='str')

        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        file_system.save_content_in_file('', excel_buffer.read(), f'{table}.xlsx')


matrices_all_assets_job = define_asset_job(name="matrices_all_assets_job")

@asset_sensor(
    asset_key=AssetKey('combined_05_location'),
    job=matrices_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def run_matrices_sensor(context: SensorEvaluationContext):
    # Get the last run status of the job
    job_to_look = 'matrices_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # Check if the last run is finished
    if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
        return SkipReason(f"Last run status is {last_run_status}")

    return RunRequest()

# Failure sensor that sends a message to slack
matrices_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[matrices_all_assets_job],
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