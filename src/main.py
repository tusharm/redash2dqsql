import os
import sys
from pprint import pprint

from transform import transform_query, fix_query_params
from redash import RedashClient
from dbsql import DBXClient
from dotenv import load_dotenv


def run(redash: RedashClient, dbx: DBXClient):
    # Get queries for a dashboard
    dashboards = redash.dashboards(tags=['tradie_engagement'])
    for dashboard in list(dashboards):
        dashboard_info = redash.get_dashboard(dashboard['id'])
        dashboard_id = dashboard_info['id']
        dashboard_widgets = dashboard_info['widgets']
        for widget_options in dashboard_widgets:
            widget_text = widget_options['text']
            widget_width = widget_options['width']
            visualization_id = widget_options['visualization']['id']
        #
            dbx.create_widget(dashboard_id=dashboard_id,
                              widget_options=widget_options,
                              visualization_id=visualization_id,
                              text=widget_text,
                              width=widget_width)
        # print(redash.get_dashboard(dashboard['id']))
        # for query in redash.queries_for(dashboard):
            # pass
            # query = redash.queries_for(dashboard)[0]

            # Get queries by tag
            # query = redash.queries(tags=['TC'])[0]

            # Convert to Databricks format

            # print(query)
            # transform_query(query)
            #
            # dbx_id = dbx.create_query(query, target_folder='/folders/3220363672964329')
            # pprint(dbx.get_query(dbx_id))
        # dash_info = dbx.get_dashboard(dashboard['id'])
        # print(dash_info)
        # dbx.create_dashboard(dashboard_name=dashboard['name'], target_folder='')


if __name__ == '__main__':
    load_dotenv()
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
