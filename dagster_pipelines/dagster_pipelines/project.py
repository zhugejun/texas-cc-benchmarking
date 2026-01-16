from pathlib import Path

from dagster_dbt import DbtProject

texas_cc_benchmarking_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "texas_cc_benchmarking").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
texas_cc_benchmarking_project.prepare_if_dev()