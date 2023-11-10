from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine

from .constants import dbt_manifest_path

@asset(compute_kind="python")
def einstein_raw(context):
    """Get top stories from the HackerNews top stories endpoint.

    API Docs: https://github.com/HackerNews/API#new-top-and-best-stories
    """
    root_path = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
    einstein_path = root_path / "data" / "einstein"

    engine = create_engine('postgresql://itps_dev:itps_dev@localhost:5433/itps_dev')

    for file in os.listdir(einstein_path):

        if not file.endswith('.xlsx'):
            continue

        print(einstein_path / file)
        einstein_df = pd.read_excel(einstein_path / file, dtype = str, sheet_name='DENGUE')
        einstein_df['file_name'] = file
        einstein_df.to_sql('einstein_raw', engine, schema='arboviroses', if_exists='replace', index=False)

    engine.dispose()

    context.add_output_metadata({'num_rows': einstein_df.shape[0]})

@dbt_assets(manifest=dbt_manifest_path, select='einstein')
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
