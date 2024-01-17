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

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='combined +epiweeks +municipios +age_groups +fix_location +macroregions',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([arboviroses_dbt_assets], "combined_05_location")]
)
def export_to_xlsx(context):
    """
    Get the final combined data from the database and export to xlsx
    """
    # Create data folder if not exists
    pathlib.Path('data/combined').mkdir(parents=True, exist_ok=True)

    # Export to xlsx
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    df = pd.read_sql('select * from arboviroses."combined_05_location"', engine)
    df.to_excel('data/combined/combined.xlsx', index=False)
    engine.dispose()

    context.add_output_metadata({
        'num_rows': df.shape[0],
        # 'laboratories': df['laboratory'].nunique(),
    })

