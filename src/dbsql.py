from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryOptions, Parameter, ParameterType, DashboardsAPI, RunAsRole, WidgetOptions

from redash import Query


class DBXClient:
    def __init__(self, url, token):
        self.client = WorkspaceClient(host=url, token=token)

        warehouse = self.client.data_sources.list()[0]
        self.warehouse_id = warehouse.id

        # TODO: ideally this should be external eg a Delta table
        self.cache = dict()

    def get_query(self, id):
        return self.client.queries.get(id)

    def create_query(self, query: Query, target_folder: str) -> str:
        """
        Given a Query model, creates a Databricks query at the target location.

        If the query depends on other queries (see https://docs.databricks.com/en/sql/user/queries/query-parameters.html#query-based-dropdown-list),
        those dependencies are created first.

        Also, caches mapping of migrated queries to enable re-use
        """
        for q in query.depends_on:
            self.create_query(q, target_folder)

        cached_id = self.read_cache(query.id)
        if cached_id:
            return cached_id

        # currently, API doesn't support attaching tags!
        created = self.client.queries.create(
            name=query.name,
            data_source_id=self.warehouse_id,
            description=f"Migrated from Redash on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, tags: {','.join(query.tags)}",
            query=query.query_string,
            parent=target_folder,
            options=self._build_options(query)
        )

        self.update_cache(query.id, created.id)
        return created.id

    def create_dashboard(self, dashboard_name: str, target_folder: str, tags=None, is_favorite=False,
                         run_as_role=RunAsRole.VIEWER,
                         dashboard_filters_enabled=True):
        """
        Create a Databricks dashboard using the Lakeview API.

        Args:
            dashboard_name (str): Name of the dashboard.
            target_folder (str): ID or path of the parent folder where the dashboard will be created.
            tags (list, optional): List of tags for the dashboard.
            is_favorite (bool, optional): Set to True if the dashboard should be marked as a favorite.
            run_as_role (str, optional): Role under which the dashboard will run (e.g., "viewer").
            dashboard_filters_enabled (bool, optional): Set to True to enable dashboard filters.

        Returns:
            dict: Response from the create_dashboard API.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        # Create the dashboard in the draft state
        created_dashboard = self.client.dashboards.create(
            name=dashboard_name,
            parent=target_folder,
            tags=tags,
            is_favorite=is_favorite,
            run_as_role=run_as_role,
            dashboard_filters_enabled=dashboard_filters_enabled
        )

    def create_widget(self, dashboard_id, visualization_id, widget_options, text, width):
        """
        Create a widget in a Databricks dashboard using the DashboardWidgetsAPI.

        Args:
            dashboard_id (str): ID of the Databricks dashboard where the widget will be created.
            visualization_id (str): ID of the visualization associated with the widget.
            text (str): Text content of the widget.
            widget_options (dict): Widget options.

        Returns:
            dict: Response from the create_widget API.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        def _build_widget_options(widget_options):
            options = {'parameterMappings': widget_options['options']['parameterMappings'],
                       'isHidden': widget_options['options']['isHidden'],
                       'position': widget_options['options']['position'],
                       'created_at': widget_options['created_at'],
                       'updated_at': widget_options['updated_at']
                       }

            test = WidgetOptions.from_dict(options)
            return test

        # Create the widget
        created_widget = self.client.dashboard_widgets.create(
            dashboard_id=dashboard_id,
            options=_build_widget_options(widget_options),
            width=width,
            text=text,
            visualization_id=visualization_id
        )

    def get_dashboard(self, dashboard_id: str):
        """
        Retrieve a Databricks dashboard.

        Args:
            dashboard_id (str): UUID identifying the dashboard to be retrieved.

        Returns:
            dict: JSON representation of the retrieved dashboard object.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        # Call the get_dashboard API
        return self.client.dashboards.get(dashboard_id)

    def read_cache(self, redash_query_id: int) -> str:
        """
        Looks up a cache to see if this query has been already migrated
        """

        return self.cache.get(redash_query_id, None)

    def update_cache(self, redash_id: int, dbx_id: int):
        """
        Update the cache, so we can find the ID later
        """
        self.cache[redash_id] = dbx_id

    def _build_options(self, query) -> dict:
        """
        Builds Databricks query options from Redash query model
        """

        def build_parameter(params: dict) -> Parameter:
            p = Parameter.from_dict(params)

            # TODO:
            # https://github.com/databricks/databricks-sdk-py/issues/475
            # Once query-based parameters are supported, lookup ID of the referenced query in the cache
            # and replace it in `queryId`

            # if parameter type is not supported, default it to TEXT
            if not p.type:
                p.type = ParameterType.TEXT
                p.value = str(p.value)
            return p

        return QueryOptions(
            parameters=[
                build_parameter(p)
                for p in query.options.get('parameters', [])
                if p['name'] in query.params
            ]
        ).as_dict()
