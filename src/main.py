import os
import sys

from redash_toolbelt import Redash

from extract import Client, get_queries
from convert import convert_query


def run(url, api_key):
    client = Client(Redash(url, api_key))

    dashboards = client.get_dashboards(tags=['Job estimator'])
    job_estimator = list(dashboards)[0]
    queries = get_queries(job_estimator)

    print(convert_query(queries[0]))


if __name__ == '__main__':
    api_key = os.getenv("REDASH_API_KEY")
    if not api_key:
        sys.exit("Missing env var 'REDASH_API_KEY'")

    url = os.getenv("REDASH_URL")
    if not url:
        sys.exit("Missing env var 'REDASH_URL'")

    run(url, api_key)
