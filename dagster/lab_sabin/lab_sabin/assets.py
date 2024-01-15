from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets
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

@asset(compute_kind="python")
def sabin_raw(context):
    root_path = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
    sabin_path = root_path / "data" / "sabin"

    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    sabin_df = pd.DataFrame()
    for file in os.listdir(sabin_path):
        if not file.endswith('.csv'):
            continue

        df = pd.read_csv(sabin_path / file, dtype = str)
        
        df['file_name'] = file
        sabin_df = pd.concat([sabin_df, df], ignore_index=True)

    # Save to db
    sabin_df.to_sql('sabin_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': sabin_df.shape[0]})


@dbt_assets(manifest=dbt_manifest_path, select='sabin')
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()