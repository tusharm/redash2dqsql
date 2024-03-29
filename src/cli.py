from __future__ import annotations

import traceback

import click


@click.group
@click.version_option(version='1.0.0')
@click.option('--redash-url', help='Redash URL', envvar='REDASH_URL')
@click.option('--redash-api-key', help='Redash API Key', envvar='REDASH_API_KEY')
@click.option('--databricks-host', help='Redash API Key', envvar='DATABRICKS_HOST')
@click.option('--databricks-token',  help='Redash API Key', envvar='DATABRICKS_TOKEN')
@click.pass_context
def cli(ctx, redash_url, redash_api_key, databricks_host, databricks_token):
    ctx.ensure_object(dict)
    ctx.obj['redash_url'] = redash_url
    ctx.obj['redash_api_key'] = redash_api_key
    ctx.obj['databricks_host'] = databricks_host
    ctx.obj['databricks_token'] = databricks_token


def check_required_options(ctx):
    """
    Extract check for required options into a function to enable --help function to work
    """
    if not all([ctx.obj['redash_url'], ctx.obj['redash_api_key'], ctx.obj['databricks_host'], ctx.obj['databricks_token']]):
        click.echo("""
        Missing required options to connect to redash and databricks:
        --redash-url
        --redash-api-key
        --databricks-host
        --databricks-token
        
        Pass them as arguments or set them as environment variables:
        REDASH_URL
        REDASH_API_KEY
        DATABRICKS_HOST
        DATABRICKS_TOKEN
        """)
        click.echo("Run 'redash2databricks --help' for usage.")
        raise click.Abort()


@cli.command()
@click.pass_context
@click.argument('target-folder', type=click.Path(file_okay=False, dir_okay=True, path_type=str))
@click.option('--alert-id', help='Alert ID', default=None)
@click.option('--tags', help='Tags to filter on', multiple=True, default=None)
@click.option('--destination-id', help='Destination ID', default=None)
@click.option('--warehouse-id', help='SQL Warehouse ID', default=None)
@click.option('--run-as', help='User or the service principle to run the alert job as.', default=None)
@click.option('--source-dialect', help='Source query SQL dialect', default=None)
@click.option('--no-sqlglot', help='Disable SQL glot based query transformations', default=False, is_flag=True)
def alerts(ctx, target_folder, alert_id, tags, destination_id, warehouse_id, run_as, source_dialect, no_sqlglot):
    check_required_options(ctx)
    from redash import RedashClient
    from dbsql import DBXClient
    from transform import transform_query

    redash = RedashClient(ctx.obj['redash_url'], ctx.obj['redash_api_key'])
    dbx = DBXClient(ctx.obj['databricks_host'], ctx.obj['databricks_token'])

    alerts_list = redash.alerts(tags=tags, alert_id=alert_id)
    for alert in alerts_list:
        if not no_sqlglot:
            if source_dialect:
                transform_query(alert.query, source_dialect)
            else:
                transform_query(alert.query)
        try:
            dbx_id = dbx.create_alert(
                alert,
                target_folder,
                destination_id=destination_id,
                warehouse_id=warehouse_id,
                run_as=run_as
            )
            click.echo(f"Created alert {dbx_id}")
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            click.echo(e)
            raise click.Abort(e)


if __name__ == '__main__':
    cli(obj={})
