import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import arboviroses_dbt_assets, sabin_raw
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets, sabin_raw],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)