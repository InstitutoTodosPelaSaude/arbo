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
from sqlalchemy import create_engine
from dotenv import load_dotenv

from .constants import dbt_manifest_path
from .generate_matrices import generate_country_epiweek_matrix, generate_country_agegroup_matrix, generate_state_epiweek_matrix

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

PATHOGENS = ['DENV']#['CHIKV', 'DENV', 'MAYV', 'OROV', 'WNV', 'YFV', 'ZIKV',]

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
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_pos_neg_results")
    ]
)
def country_epiweek_matrices(context):
    """
    Generate country matrices for each pathogen and metric combination.
    """
    METRICS = ['PosNeg', 'Pos', 'totaltests', 'posrate']
    for pathogen in PATHOGENS:
        for metric in METRICS:
            generate_country_epiweek_matrix(
                cube_db_table='matrix_02_CUBE_pos_neg_results',
                pathogen=pathogen,
                metric=metric,
                show_testkits=True,
                matrix_name=f'matrix_{pathogen.upper()}_country_{metric.lower()}_testkits_weeks_noigg'
            )
            generate_country_epiweek_matrix(
                cube_db_table='matrix_02_CUBE_pos_neg_results',
                pathogen=pathogen,
                metric=metric,
                show_testkits=False,
                matrix_name=f'matrix_{pathogen.upper()}_country_{metric.lower()}_direct_weeks_noigg'
            )

@asset(
    compute_kind="python", 
    deps=[
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_pos_neg_results"),
    ]
)
def state_epiweek_matrices(context):
    """
    Generate state matrices for each pathogen.
    """
    for pathogen in PATHOGENS:
        generate_state_epiweek_matrix(
            cube_db_table='matrix_02_CUBE_pos_neg_results',
            pathogen=pathogen,
            matrix_name=f'matrix_{pathogen.upper()}_state_posneg_testkits_weeks_noigg'
        )

@asset(
    compute_kind="python", 
    deps=[
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_pos_neg_results")
    ]
)
def country_agegroup_matrices():
    """
    Generate agegroup matrix for all pathogens and PosNeg metric.
    """
    generate_country_agegroup_matrix(
        cube_db_table='matrix_02_CUBE_pos_neg_results',
        matrix_name='matrix_ALL_country_agegroup_noigg'
    )

@asset(
    compute_kind="python", 
    deps=[
        country_epiweek_matrices,
        state_epiweek_matrices,
        country_agegroup_matrices,
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
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_CHIKV_pos_direct_states")
    ]
)
def export_matrices_to_tsv():
    """
    Export all matrices to TSV files. The TSV files are saved to the `matrices` folder.
    """

    file_names = {'matrix_NEW_DENV_posrate_by_epiweek_year':'01_DENV_line_posrate_direct_week_country_years', 
                  'matrix_NEW_DENV_posrate_pos_neg_by_epiweek':'02_DENV_line_bar_posrate_posneg_direct_week_country',
                  'matrix_NEW_DENV_totaltests_by_epiweek_region':'03_DENV_bar_total_direct_weeks_regions',
                  'matrix_NEW_DENV_posrate_by_epiweek_state_filtered':'04_DENV_line_posrate_direct_weeks_states',
                  'matrix_NEW_DENV_posrate_by_epiweek_agegroups':'06_DENV_heat_posrate_agegroups_week_country',
                  'matrix_NEW_DENV_pos_by_month_agegroups_renamed':'07_DENV_barH_pos_agegroups_month_country',
                  'matrix_NEW_ARBO_pos_by_month_agegroups_renamed':'08_Arbo_barH_pos_agegroups_month_country',
                  'matrix_NEW_DENV_pos_direct_states':'11_DENV_map_pos_direct_states',
                  'matrix_NEW_DENV_pos_direct_cities':'11_DENV_map_pos_direct_cities',
                  'matrix_NEW_CHIKV_pos_direct_states':'12_CHIKV_map_pos_direct_states',
                  'matrix_NEW_CHIKV_pos_direct_cities':'12_CHIKV_map_pos_direct_cities'
                  }

    
    # Connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # List of matrix tables
    matrix_tables = ['matrix_NEW_DENV_posrate_by_epiweek_year', 'matrix_NEW_DENV_posrate_pos_neg_by_epiweek', 
                     'matrix_NEW_DENV_totaltests_by_epiweek_region', 'matrix_NEW_DENV_posrate_by_epiweek_state_filtered', 
                     'matrix_NEW_DENV_posrate_by_epiweek_agegroups', 'matrix_NEW_DENV_pos_by_month_agegroups_renamed',
                     'matrix_NEW_ARBO_pos_by_month_agegroups_renamed', 'matrix_NEW_DENV_pos_direct_cities', 'matrix_NEW_DENV_pos_direct_states',
                     'matrix_NEW_CHIKV_pos_direct_cities', 'matrix_NEW_CHIKV_pos_direct_states',
                     'matrix_NEW_ALL_pos_by_month_agegroups_renamed', 'matrix_NEW_DENV_pos_by_epiweek_state',
                     'matrix_NEW_ALL_pos_by_month_agegroups', 'matrix_NEW_DENV_posrate_by_epiweek_state'
                     ]

    # Create the matrices folder if it doesn't exist
    path = 'data/matrices'
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    # Export each matrix table to a TSV file
    for table in matrix_tables:
        df = pd.read_sql_query(f'SELECT * FROM arboviroses."{table}"', engine)
        df = df.fillna(0)

        try:
            new_name = file_names[table]
            df.to_csv(f'{path}/{new_name}.tsv', sep='\t', index=False)
        except:
            df.to_csv(f'{path}/{table}.tsv', sep='\t', index=False)

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
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_NEW_CHIKV_pos_direct_states")
    ]
)
def export_matrices_to_xlsx():
    """
    Export all new matrices to XLSX files. The XLSX files are saved to the `matrices` folder.
    """

    file_names = {'matrix_NEW_DENV_posrate_by_epiweek_year':'01_DENV_line_posrate_direct_week_country_years', 
                  'matrix_NEW_DENV_posrate_pos_neg_by_epiweek':'02_DENV_line_bar_posrate_posneg_direct_week_country',
                  'matrix_NEW_DENV_totaltests_by_epiweek_region':'03_DENV_bar_total_direct_weeks_regions',
                  'matrix_NEW_DENV_posrate_by_epiweek_state_filtered':'04_DENV_line_posrate_direct_weeks_states',
                  'matrix_NEW_DENV_posrate_by_epiweek_agegroups':'06_DENV_heat_posrate_agegroups_week_country',
                  'matrix_NEW_DENV_pos_by_month_agegroups_renamed':'07_DENV_barH_pos_agegroups_month_country',
                  'matrix_NEW_ARBO_pos_by_month_agegroups_renamed':'08_Arbo_barH_pos_agegroups_month_country',
                  'matrix_NEW_DENV_pos_direct_states':'11_DENV_map_pos_direct_states',
                  'matrix_NEW_DENV_pos_direct_cities':'11_DENV_map_pos_direct_cities'
                  }
    
    # Connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # List of matrix tables
    matrix_tables = ['matrix_NEW_DENV_posrate_by_epiweek_year', 'matrix_NEW_DENV_posrate_pos_neg_by_epiweek', 
                     'matrix_NEW_DENV_totaltests_by_epiweek_region', 'matrix_NEW_DENV_posrate_by_epiweek_state_filtered', 
                     'matrix_NEW_DENV_posrate_by_epiweek_agegroups', 'matrix_NEW_DENV_pos_by_month_agegroups_renamed',
                     'matrix_NEW_ARBO_pos_by_month_agegroups_renamed', 'matrix_NEW_DENV_pos_direct_cities', 'matrix_NEW_DENV_pos_direct_states',
                     'matrix_NEW_CHIKV_pos_direct_cities', 'matrix_NEW_CHIKV_pos_direct_states',
                     'matrix_NEW_ALL_pos_by_month_agegroups_renamed', 'matrix_NEW_DENV_pos_by_epiweek_state',
                     'matrix_NEW_ALL_pos_by_month_agegroups', 'matrix_NEW_DENV_posrate_by_epiweek_state'
                     ]

    # Create the matrices folder if it doesn't exist
    path = 'data/matrices'
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    # Export each matrix table to a TSV file
    for table in matrix_tables:
        df = pd.read_sql_query(f'SELECT * FROM arboviroses."{table}"', engine, dtype='str')
        
        df = df.fillna(0)
        df = df.fillna('0')
        df = df.fillna('0.0')

        try:
            new_name = file_names[table]
            df.to_excel(f'{path}/{new_name}.xlsx', index=False)
        except:
            df.to_excel(f'{path}/{table}.xlsx', index=False)


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
    text_fn = lambda context: f"MATRICES JOB FAILED: {context.failure_event.message}"
)