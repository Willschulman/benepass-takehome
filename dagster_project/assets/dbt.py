from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from dagster_project.constants import DBT_PROJECT_DIR

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def benepass_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Materialize all dbt models as Dagster assets."""
    yield from dbt.cli(["build"], context=context).stream()
