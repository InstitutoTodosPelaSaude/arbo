import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    dbmol_raw,
    dbmol_remove_used_files,
    dbmol_all_assets_job,
    new_dbmol_file_sensor,
    dbmol_slack_failure_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[
        arboviroses_dbt_assets, 
        dbmol_raw,
        dbmol_remove_used_files
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[dbmol_all_assets_job],
    sensors=[new_dbmol_file_sensor, dbmol_slack_failure_sensor]
)