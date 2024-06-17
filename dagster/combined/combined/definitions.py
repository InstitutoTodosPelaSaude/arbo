import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    export_to_tsv,
    zip_exported_file,
    combined_all_assets_job,
    run_combined_sensor,
    combined_slack_failure_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, export_to_tsv, zip_exported_file],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[combined_all_assets_job],
    sensors=[run_combined_sensor, combined_slack_failure_sensor]
)