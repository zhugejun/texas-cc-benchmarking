import yaml
from pathlib import Path
from dagster import ConfigurableResource
from snowflake.connector import connect
from pydantic import Field


class SnowflakeResource(ConfigurableResource):
    """Snowflake connection resource for data ingestion."""
    
    account: str = Field(description="Snowflake account identifier")
    user: str = Field(description="Snowflake username")
    password: str = Field(description="Snowflake password")
    warehouse: str = Field(default="COMPUTE_WH", description="Snowflake warehouse")
    database: str = Field(default="TEXAS_CC", description="Snowflake database")
    schema_name: str = Field(default="RAW_IPEDS", description="Snowflake schema")
    role: str = Field(default="ACCOUNTADMIN", description="Snowflake role")
    
    def get_connection(self):
        """Create and return a Snowflake connection."""
        return connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_name,
            role=self.role,
        )


def get_snowflake_config_from_dbt():
    """
    Get Snowflake configuration from dbt profiles.yml.
    
    Reads the texas_cc_benchmarking profile from ~/.dbt/profiles.yml
    and extracts Snowflake connection details.
    """
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    
    if not profiles_path.exists():
        raise FileNotFoundError(
            f"dbt profiles.yml not found at {profiles_path}. "
            "Please run 'dbt init' to configure your dbt profile first."
        )
    
    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    
    # Get the texas_cc_benchmarking profile
    if 'texas_cc_benchmarking' not in profiles:
        raise ValueError(
            "Profile 'texas_cc_benchmarking' not found in profiles.yml. "
            "Available profiles: " + ", ".join(profiles.keys())
        )
    
    profile = profiles['texas_cc_benchmarking']
    target_name = profile.get('target', 'dev')
    target = profile['outputs'][target_name]
    
    # Extract Snowflake credentials
    return {
        "account": target['account'],
        "user": target['user'],
        "password": target.get('password', ''),
        "warehouse": target.get('warehouse', 'COMPUTE_WH'),
        "database": target.get('database', 'TEXAS_CC'),
        "schema_name": target.get('schema', 'RAW_IPEDS'),
        "role": target.get('role', 'ACCOUNTADMIN'),
    }
