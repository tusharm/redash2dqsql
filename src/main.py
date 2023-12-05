import os
import sys

from convert import convert_query
from extract import RedashClient, get_queries
from upload import DBXClient


def migrate(redash: RedashClient, dbx: DBXClient):
    dashboards = redash.get_dashboards(tags=['Job estimator'])
    job_estimator = list(dashboards)[0]
    queries = get_queries(job_estimator)

    print(convert_query(queries[0]))


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

    migrate(redash, dbx)
