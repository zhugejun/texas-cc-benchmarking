from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    ipeds_hd,
    ipeds_c_a,
    ipeds_effy,
    ipeds_gr,
    ipeds_sfa,
    ipeds_ef_d,
    texas_cc_benchmarking_dbt_assets,
)
from .project import texas_cc_benchmarking_project
from .resources import SnowflakeResource, get_snowflake_config_from_dbt
from .schedules import schedules

defs = Definitions(
    assets=[
        # IPEDS ingestion assets
        ipeds_hd,
        ipeds_c_a,
        ipeds_effy,
        ipeds_gr,
        ipeds_sfa,
        ipeds_ef_d,
        # dbt transformation assets
        texas_cc_benchmarking_dbt_assets,
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=texas_cc_benchmarking_project),
        "snowflake": SnowflakeResource(**get_snowflake_config_from_dbt()),
    },
)