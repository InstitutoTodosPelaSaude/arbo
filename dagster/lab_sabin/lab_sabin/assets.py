from dagster import (
    AssetExecutionContext, 
    asset,
    sensor, 
    define_asset_job, 
    RunRequest, 
    SkipReason, 
    SensorEvaluationContext,
    DefaultSensorStatus
)
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    get_asset_key_for_model
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import requests
import shutil

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
SABIN_FILES_FOLDER = ROOT_PATH / "data" / "sabin"
SABIN_RAW_FILES_EXTENSION = '.xlsx'
SABIN_CONVERTED_FILES_EXTENSION = '.csv'

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
def sabin_convert_xlsx_to_csv(context):
    for file in os.listdir(SABIN_FILES_FOLDER):
        if not file.endswith(SABIN_RAW_FILES_EXTENSION):
            continue
        
        file_path = SABIN_FILES_FOLDER / file
        response = requests.post(
            "http://xlsx2csv:2140/convert",
            files={"file": open(file_path, "rb")},
        )

        if response.status_code != 200:
            raise Exception(f"Error converting file {file_path}")
        
        with open(file_path.with_suffix(SABIN_CONVERTED_FILES_EXTENSION), 'wb') as f:
            f.write(response.content)

        context.log.info(f"Converted file {file_path}")

        # Move the original file to _out folder
        shutil.move(file_path, SABIN_FILES_FOLDER / '_out' / file)
        context.log.info(f"Moved file {file_path} to _out folder")


@asset(compute_kind="python", deps=[sabin_convert_xlsx_to_csv])
def sabin_raw(context):
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    sabin_df = pd.DataFrame()
    for file in os.listdir(SABIN_FILES_FOLDER):
        if not file.endswith(SABIN_CONVERTED_FILES_EXTENSION):
            continue

        df = pd.read_csv(SABIN_FILES_FOLDER / file, dtype = str)
        
        df['file_name'] = file
        sabin_df = pd.concat([sabin_df, df], ignore_index=True)

    # Save to db
    sabin_df.to_sql('sabin_raw', engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': sabin_df.shape[0]})

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='sabin',
        dagster_dbt_translator=dagster_dbt_translator
)
def arboviroses_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()