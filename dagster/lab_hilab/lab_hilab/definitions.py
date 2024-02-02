import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    hilab_raw, 
    hilab_all_assets_job, 
    new_hilab_file_sensor, 
    hilab_remove_used_files
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, hilab_raw, hilab_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[hilab_all_assets_job],
    sensors=[new_hilab_file_sensor],
)