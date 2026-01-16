from setuptools import find_packages, setup

setup(
    name="dagster_pipelines",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_pipelines": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core<1.12",
        "dbt-duckdb<1.12",
        "dbt-duckdb<1.12",
        "dbt-snowflake<1.12",
        "dbt-snowflake<1.12",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)