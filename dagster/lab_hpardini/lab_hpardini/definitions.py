import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    hpardini_raw,
    hpardini_remove_used_files,
    hpardini_all_assets_job,
    new_hpardini_file_sensor,
    hpardini_slack_failure_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, hpardini_raw, hpardini_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[hpardini_all_assets_job],
    sensors=[new_hpardini_file_sensor, hpardini_slack_failure_sensor],
)