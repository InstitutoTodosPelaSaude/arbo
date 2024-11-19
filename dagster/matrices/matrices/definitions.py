import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import *
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets,
            export_matrices_to_xlsx],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[matrices_all_assets_job],
    sensors=[run_matrices_sensor, matrices_slack_failure_sensor]
)