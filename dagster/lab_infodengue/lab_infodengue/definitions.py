import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    infodengue_raw, 
    infodengue_remove_used_files, 
    export_to_csv,

    infodengue_all_assets_job,
    infodengue_slack_failure_sensor,
    new_infodengue_file_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[
        arboviroses_dbt_assets, 
        infodengue_raw,
        export_to_csv,
        infodengue_remove_used_files
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[infodengue_all_assets_job],
    sensors=[new_infodengue_file_sensor, infodengue_slack_failure_sensor]
)