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
def hlagyn_raw(context):
    """
    Table with the raw data from hlagyn
    """
    root_path = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
    hlagyn_path = root_path / "data" / "hlagyn"

    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    hlagyn_df = pd.DataFrame()
    for file in os.listdir(hlagyn_path):
        if not file.endswith('.xlsx'):
            continue

        print(hlagyn_path / file)
        df = pd.read_excel(hlagyn_path / file, dtype = str, sheet_name='DENGUE') # TODO: change sheet name
        df['file_name'] = file
        einstein_df = pd.concat([einstein_df, df], ignore_index=True)
        
    # Save to db
    hlagyn_df.to_sql('hlagyn_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': hlagyn_df.shape[0]})

@dbt_assets(manifest=dbt_manifest_path, select='hlagyn')
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()