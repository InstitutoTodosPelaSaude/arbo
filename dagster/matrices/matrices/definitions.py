import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import *
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets,
            country_epiweek_matrices,
            country_agegroup_matrices],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)