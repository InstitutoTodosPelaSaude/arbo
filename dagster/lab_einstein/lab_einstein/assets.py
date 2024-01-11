from dagster import AssetExecutionContext, asset, MaterializeResult, MetadataValue
from textwrap import dedent

from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
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

@asset(compute_kind="python")
def einstein_raw(context):
    """
    Read all excel files from data/einstein folder and save to db
    """
    root_path = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
    einstein_path = root_path / "data" / "einstein"

    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    einstein_df = pd.DataFrame()
    for file in os.listdir(einstein_path):
        if not file.endswith('.xlsx'):
            continue

        print(einstein_path / file)
        df = pd.read_excel(einstein_path / file, dtype = str, sheet_name='itps_dengue')
        df['file_name'] = file
        einstein_df = pd.concat([einstein_df, df], ignore_index=True)
        
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
