import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets,
    new_target_file_sensor,
    target_slack_failure_sensor,
    target_all_assets_job,
    # target_remove_used_files,
    target_raw
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
#    assets=[arboviroses_dbt_assets, target_raw, target_remove_used_files],
    assets=[arboviroses_dbt_assets, target_raw],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[target_all_assets_job],
    sensors=[new_target_file_sensor, target_slack_failure_sensor],
)