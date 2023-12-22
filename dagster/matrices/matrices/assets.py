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
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_test_kit__epiweek__pathogen")]
)
def country_posrate_direct_weeks(context):
    """
    Generate matrices from the database and export to tsv
    """
    for pathogen in PATHOGENS:
        # Build query
        query = f"""
            SELECT
                pathogen,
                epiweek_enddate,
                positivity_rate
            FROM arboviroses."matrix_02_CUBE_test_kit__epiweek__pathogen"
            WHERE
                pathogen = '{pathogen}' AND
                test_kit IS NULL AND
                epiweek_enddate IS NOT NULL
        """

        # Get results from database
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df = pd.read_sql(query, engine)

        # Asserts
        assert df.epiweek_enddate.nunique() == df.shape[0], 'There are duplicated dates'

        # Transform data to the right format
        df = df.set_index(['pathogen', 'epiweek_enddate']).unstack('epiweek_enddate').reset_index()
        new_columns = [(col[0], col[0]) if col[1] == '' else col for col in df.columns.to_list()]
        df.columns = pd.MultiIndex.from_tuples(new_columns).droplevel(0)

        # Create or manipulate custom columns
        df.insert(1, f'{pathogen}_test_result', 'Pos')

        # Save in the database
        df.to_sql(f'matrix_{pathogen}_country_posrate_direct_weeks', engine, schema='arboviroses', if_exists='replace', index=False)

    engine.dispose()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "matrix_02_CUBE_test_kit__epiweek__pathogen")]
)
def country_totaltests_direct_weeks(context):
    """
    Generate matrices from the database and export to tsv
    """
    for pathogen in PATHOGENS:
        # Build query
        query = f"""
            SELECT
                pathogen,
                epiweek_enddate,
                total_lines
            FROM arboviroses."matrix_02_CUBE_test_kit__epiweek__pathogen"
            WHERE
                pathogen = '{pathogen}' AND
                test_kit IS NULL AND
                epiweek_enddate IS NOT NULL
        """

        # Get results from database
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df = pd.read_sql(query, engine)

        # Asserts
        assert df.epiweek_enddate.nunique() == df.shape[0], 'There are duplicated dates'

        # Transform data to the right format
        df = df.set_index(['pathogen', 'epiweek_enddate']).unstack('epiweek_enddate').reset_index()
        new_columns = [(col[0], col[0]) if col[1] == '' else col for col in df.columns.to_list()]
        df.columns = pd.MultiIndex.from_tuples(new_columns).droplevel(0)

        # Save in the database
        df.to_sql(f'matrix_{pathogen}_country_totaltests_direct_weeks', engine, schema='arboviroses', if_exists='replace', index=False)

    engine.dispose()