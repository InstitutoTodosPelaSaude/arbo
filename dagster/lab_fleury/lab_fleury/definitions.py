import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets,
    fleury_raw,
    fleury_remove_used_files,
    fleury_all_assets_job,
    new_fleury_file_sensor,
    fleury_slack_failure_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, fleury_raw, fleury_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[fleury_all_assets_job],
    sensors=[new_fleury_file_sensor, fleury_slack_failure_sensor],
)