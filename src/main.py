import os
import sys
from pprint import pprint

from transform import transform_query
from redash import RedashClient
from dbsql import DBXClient


def run(redash: RedashClient, dbx: DBXClient):
    # Get queries for a dashboard
    dashboards = redash.dashboards(tags=['Job estimator'])
    job_estimator = list(dashboards)[0]
    query = redash.queries_for(job_estimator)[0]

    # Get queries by tag
    # query = redash.queries(tags=['TC'])[0]

    # Convert to Databricks format
    transform_query(query)
    pprint(query)

    dbx_id = dbx.create_query(query, target_folder="/")
    pprint(dbx.get_query(dbx_id))


if __name__ == '__main__':
    redash_key = os.getenv("REDASH_API_KEY")
    if not redash_key:
        sys.exit("Missing env var 'REDASH_API_KEY'")

    redash_url = os.getenv("REDASH_URL")
    if not redash_url:
        sys.exit("Missing env var 'REDASH_URL'")

    redash = RedashClient(redash_url, redash_key)

    dbx_host = os.getenv("DATABRICKS_HOST")
    if not dbx_host:
        sys.exit("Missing env var 'DATABRICKS_HOST'")

    dbx_token = os.getenv("DATABRICKS_TOKEN")
    if not dbx_token:
        sys.exit("Missing env var 'DATABRICKS_TOKEN'")

    dbx = DBXClient(dbx_host, dbx_token)

    run(redash, dbx)
