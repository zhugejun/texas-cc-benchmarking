from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import texas_cc_benchmarking_dbt_assets
from .project import texas_cc_benchmarking_project
from .schedules import schedules

defs = Definitions(
    assets=[texas_cc_benchmarking_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=texas_cc_benchmarking_project),
    },
)