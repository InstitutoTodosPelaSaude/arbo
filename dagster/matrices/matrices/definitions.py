import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import *
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[arboviroses_dbt_assets,
            country_posrate_direct_weeks,
            country_totaltests_direct_weeks,
            country_posneg_direct_weeks,
            country_pos_direct_weeks,
            country_posneg_testkits_weeks],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)