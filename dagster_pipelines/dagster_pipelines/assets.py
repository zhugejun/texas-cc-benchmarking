from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import texas_cc_benchmarking_project


@dbt_assets(manifest=texas_cc_benchmarking_project.manifest_path)
def texas_cc_benchmarking_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    