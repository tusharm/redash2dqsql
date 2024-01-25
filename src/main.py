import os
import sys
from pprint import pprint

from transform import transform_query, fix_query_params
from redash import RedashClient
from dbsql import DBXClient
from dotenv import load_dotenv


def run(redash: RedashClient, dbx: DBXClient):
    processed_query_ids = set()
    # Get queries for a dashboard
    dashboards = redash.dashboards(tags=['tradie_engagement'])
    for dashboard in list(dashboards):
        databricks_dashboard_id = dbx.create_dashboard(dashboard_name=dashboard['name'], target_folder='')

        for query in redash.queries_for(dashboard):

            transform_query(query)
            databricks_query_id = dbx.create_query(query, target_folder='/folders/3220363672964329')
            pprint(dbx.get_query(databricks_query_id))

            redash_dashboard_info = redash.get_dashboard(dashboard['id'])
            redash_dashboard_widgets = redash_dashboard_info['widgets']
            for widget_options in redash_dashboard_widgets:
                if databricks_query_id not in processed_query_ids:
                    widget_text = widget_options['text']
                    widget_width = widget_options['width']

                    visualization_id = dbx.create_visualization(
                        query_id=databricks_query_id,
                        visualization_type=widget_options['visualization']['type'],
                        options=widget_options['options'],
                        description=widget_options['visualization']['description'],
                        name=widget_options['visualization']['name']
                    )

                    dbx.create_widget(
                        dashboard_id=databricks_dashboard_id,
                        widget_options=widget_options,
                        visualization_id=visualization_id,
                        text=widget_text,
                        width=widget_width
                    )
                    processed_query_ids.add(databricks_query_id)



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
