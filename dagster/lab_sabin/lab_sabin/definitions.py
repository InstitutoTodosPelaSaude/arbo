import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    arboviroses_dbt_assets, 
    sabin_raw, 
    sabin_convert_xlsx_to_csv,
    sabin_remove_used_files,
    sabin_all_assets_job,
    new_sabin_file_sensor
)
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, sabin_raw, sabin_convert_xlsx_to_csv, sabin_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[sabin_all_assets_job],
    sensors=[new_sabin_file_sensor]
)