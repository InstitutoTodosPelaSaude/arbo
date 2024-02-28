import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    einstein_raw,
    einstein_remove_used_files,
    einstein_all_assets_job,
    new_einstein_file_sensor,
    einstein_slack_failure_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, einstein_raw, einstein_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[einstein_all_assets_job],
    sensors=[new_einstein_file_sensor, einstein_slack_failure_sensor],
)