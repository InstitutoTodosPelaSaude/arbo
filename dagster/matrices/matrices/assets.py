from dagster import AssetExecutionContext, asset
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets, 
    get_asset_key_for_model,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings
)
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv

from .constants import dbt_manifest_path
from .generate_matrices import generate_country_epiweek_matrix, generate_country_agegroup_matrix

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

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
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_country_epiweek_withigg"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_country_epiweek_noigg")
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
                cube_db_table='matrix_02_CUBE_country_epiweek_withigg',
                pathogen=pathogen,
                metric=metric,
                show_testkits=True,
                matrix_name=f'matrix_{pathogen.upper()}_country_{metric.lower()}_testkits_weeks_withigg'
            )
            generate_country_epiweek_matrix(
                cube_db_table='matrix_02_CUBE_country_epiweek_withigg',
                pathogen=pathogen,
                metric=metric,
                show_testkits=False,
                matrix_name=f'matrix_{pathogen.upper()}_country_{metric.lower()}_direct_weeks_withigg'
            )
            generate_country_epiweek_matrix(
                cube_db_table='matrix_02_CUBE_country_epiweek_noigg',
                pathogen=pathogen,
                metric=metric,
                show_testkits=True,
                matrix_name=f'matrix_{pathogen.upper()}_country_{metric.lower()}_testkits_weeks_noigg'
            )
            generate_country_epiweek_matrix(
                cube_db_table='matrix_02_CUBE_country_epiweek_noigg',
                pathogen=pathogen,
                metric=metric,
                show_testkits=False,
                matrix_name=f'matrix_{pathogen.upper()}_country_{metric.lower()}_direct_weeks_noigg'
            )

@asset(
    compute_kind="python", 
    deps=[
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_country_agegroup_withigg"),
        get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_country_agegroup_noigg")
    ]
)
def country_agegroup_matrices():
    """
    Generate agegroup matrix for all pathogens and PosNeg metric.
    """
    generate_country_agegroup_matrix(
        cube_db_table='matrix_02_CUBE_country_agegroup_withigg',
        matrix_name='matrix_ALL_country_agegroup_withigg'
    )
    generate_country_agegroup_matrix(
        cube_db_table='matrix_02_CUBE_country_agegroup_noigg',
        matrix_name='matrix_ALL_country_agegroup_noigg'
    )