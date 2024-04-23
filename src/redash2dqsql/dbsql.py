from __future__ import annotations

from datetime import datetime
from typing import Any

from databricks.sdk import WorkspaceClient, DashboardsAPI, QueriesAPI, QueryVisualizationsAPI, DashboardWidgetsAPI
from databricks.sdk.service.sql import (
    RunAsRole,
    WidgetOptions,
)
from databricks.sdk.service.jobs import (
    CronSchedule,
    SqlTask,
    Task,
    SqlTaskAlert,
    SqlTaskSubscription,
    JobRunAs, SqlTaskQuery,
)
from databricks.sdk.service.sql import (
    QueryOptions,
    Parameter,
    ParameterType,
    AlertOptions,
)
from databricks.sdk.service.workspace import ObjectType

from redash import Query, Alert, Dashboard


class DBXClient:
    def __init__(self, url, token, warehouse_id=None):
        self.client = WorkspaceClient(host=url, token=token)

        self.dashboard_api = DashboardsAPI(self.client)
        self.queries_api = QueriesAPI(self.client)
        self.query_visualizations_api = QueryVisualizationsAPI(self.client)
        self.dashboard_widgets_api = DashboardWidgetsAPI(self.client)

        if not warehouse_id:
            warehouse = list(self.client.data_sources.list())[0]
            self.warehouse_id = warehouse.id
        else:
            self.warehouse_id = warehouse_id

        # TODO: ideally this should be external eg a Delta table
        self.cache: dict[int, tuple[str, dict[int, str]]] = dict()

    def get_query(self, id: str):
        return self.client.queries.get(id)

    def create_query(self, query: Query, target_folder: str) -> (str, dict[int, str]):
        """
        Given a Query model, creates a Databricks query at the target location.

        If the query depends on other queries (see https://docs.databricks.com/en/sql/user/queries/query-parameters.html#query-based-dropdown-list),
        those dependencies are created first.

        Also, caches mapping of migrated queries to enable re-use
        """
        for q in query.depends_on:
            self.create_query(q, target_folder)

        cached_data = self.read_cache(query.id)
        if cached_data:
            return cached_data

        # currently, API doesn't support attaching tags!
        created = self.client.queries.create(
            name=query.name,
            data_source_id=self.warehouse_id,
            description=f"Migrated from Redash on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, tags: {','.join(query.tags)}",
            query=query.query_string,
            parent=target_folder,
            options=self._build_options(query),
        )
        viz_id_map = {}
        for v in query.visualizations:
            new_viz_id = self.create_visualization(created.id, v.type.value, self._update_visualization_options(v.options), v.description, v.name)
            viz_id_map[v.id] = new_viz_id

        if not created.id:
            raise ValueError("Failed to create query")
        self.update_cache(query.id, (created.id, viz_id_map))
        return created.id, viz_id_map

    def _update_visualization_options(self, options: dict) -> dict:
        """
        Updates visualization options to match Databricks API
        """
        return options

    def create_query_schedule(self, query_id: str, schedule: dict, warehouse_id: str, run_as: str | None = None):
        """
        Creates a Databricks query schedule
        """
        run_as_obj = self.create_job_run_as(run_as)
        response = self.client.jobs.create(
            name=f"Query `{query_id}` schedule",
            description=f"Schedule for query `{query_id}` with warehouse `{warehouse_id}`",
            schedule=self._create_cron_schedule(schedule),
            run_as=run_as_obj,
            tasks=[
                Task(
                    task_key="sql",
                    sql_task=SqlTask(
                        query=SqlTaskQuery(query_id=query_id),
                        warehouse_id=warehouse_id,
                    ),
                )
            ],
        )
        if response:
            return response.job_id
        return None

    def create_dashboard(
        self,
        dashboard_name: str,
        target_folder: str,
        tags=None,
        is_favorite=False,
        run_as_role=RunAsRole.VIEWER,
        dashboard_filters_enabled=True,
    ):
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
            str: dashboard id from the create_dashboard API.

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
            dashboard_filters_enabled=dashboard_filters_enabled,
        )
        return created_dashboard.id

    def create_widget(
        self, dashboard_id, visualization_id, widget_options, text, width, title=None
    ):
        """
        Create a widget in a Databricks dashboard using the DashboardWidgetsAPI.

        Args:
            dashboard_id (str): ID of the Databricks dashboard where the widget will be created.
            visualization_id (str): ID of the visualization associated with the widget.
            text (str): Text content of the widget.
            widget_options (dict): Widget options.
            title (str, optional): Title of the widget.

        Returns:
            str: widget id from the create_widget API.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        def _build_widget_options(widget_options):
            options = {
                **widget_options
            }
            if title:
                options["title"] = title
            return WidgetOptions.from_dict(options)

        # Create the widget
        created_widget = self.client.dashboard_widgets.create(
            dashboard_id=dashboard_id,
            options=_build_widget_options(widget_options),
            width=width,
            text=text,
            visualization_id=visualization_id,
        )

        return created_widget.id

    def create_text_widget(self, dashboard_id, widget_options, text, width):
        """
        Create a text widget in a Databricks dashboard using the DashboardWidgetsAPI.

        Args:
            dashboard_id (str): ID of the Databricks dashboard where the widget will be created.
            widget_options (dict): Widget options.
            text (str): Text content of the widget.
            width (int): Width of the widget.

        Returns:
            str: widget id from the create_widget API.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        options = {
            "isHidden": widget_options["isHidden"],
            "position": widget_options["position"]
        }
        if "parameterMappings" in widget_options:
            options["parameterMappings"] = widget_options["parameterMappings"]

        widget_options_obj = WidgetOptions.from_dict(options)

        # Create the widget
        created_widget = self.client.dashboard_widgets.create(
            dashboard_id=dashboard_id,
            options=widget_options_obj,
            text=text,
            width=width
        )

        return created_widget.id

    def create_visualization(
        self, query_id, visualization_type, options, description=None, name=None
    ):
        """
        Create a visualization for a query in Databricks using the QueryVisualizationsAPI.

        Args:
            query_id (str): ID of the Databricks query where the visualization will be added.
            visualization_type (str): Type of visualization (e.g., chart, table, pivot table, etc.).
            options (dict): Visualization options.
            description (str, optional): A short description of the visualization.
            name (str, optional): The name of the visualization that appears on dashboards and the query screen.

        Returns:
            str: visualization id from the create_visualization API.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        # Create the visualization
        created_visualization = self.client.query_visualizations.create(
            query_id=query_id,
            type=visualization_type,
            options=options,
            description=description,
            name=name,
        )

        return created_visualization.id

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

    def read_cache(self, redash_query_id: int) -> tuple[str, dict[int, str]] | None:
        """
        Looks up a cache to see if this query has been already migrated
        """

        return self.cache.get(redash_query_id)

    def update_cache(self, redash_id: int, dbx_data: tuple[str, dict[int, str]]):
        """
        Update the cache, so we can find the ID later
        """
        self.cache[redash_id] = dbx_data

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
                for p in query.options.get("parameters", [])
                if p["name"] in query.params
            ]
        ).as_dict()

    def create_alert(
        self,
        alert: Alert,
        target_folder: str,
        destination_id: str | None = None,
        warehouse_id: str | None = None,
        run_as: str | None = None,
    ) -> str:
        """
        Given a Redash alert, creates a Databricks alert

        :param alert: Redash alert model
        :param target_folder: target folder to create the alert in
        :param destination_id: optional ID of the destination if schedule is set
        :param warehouse_id: optional ID of the SQL warehouse to refresh query
        :param run_as: optional user or service principle to run the alert job as
        """

        target_folder_path = f"folders/{self.get_path_object_id(target_folder)}"

        query = alert.query

        # We always create the query irrespective of whether it is cached or not to keep it as a dedicated resource
        # for the alert
        query_id = self.create_query(query, target_folder_path)[0]

        result = self._create_alert_api_call(query_id, alert, target_folder_path)
        if alert.schedule and destination_id and warehouse_id:
            self._create_alert_schedule_api_call(
                alert, result.id, destination_id, warehouse_id, run_as
            )
        return result.id

    def _create_alert_api_call(self, query_id: str, alert: Alert, parent_folder: str):
        """
        Creates an alert in Databricks
        """
        return self.client.alerts.create(
            name=alert.name,
            options=AlertOptions.from_dict(self._sanitize_alert_options(alert.options)),
            query_id=query_id,
            parent=parent_folder,
            rearm=alert.rearm,
        )

    def _sanitize_alert_options(self, options: dict) -> dict:
        """
        Sanitizes alert options to modify the keys and values to match databricks alert options
        """
        sanitized_dict = {}
        for key, value in options.items():
            if (
                key == "op"
            ):  # TODO: extend this if you see other operators not matching < > <= >= == !=
                if value == "greater than":
                    sanitized_dict["op"] = ">"
                elif value == "less than":
                    sanitized_dict["op"] = "<"
                else:
                    sanitized_dict['op'] = value
            elif key == 'value':  # The value object sometimes read as a None
                if value == 0:
                    sanitized_dict['value'] = '0'
                else:
                    sanitized_dict['value'] = value
            else:
                sanitized_dict[key] = value
        return sanitized_dict

    def _create_alert_schedule_api_call(
        self,
        alert: Alert,
        alert_id: str,
        destination_id: str,
        warehouse_id: str,
        run_as: str | None = None,
        tags: dict[str, str] = None,
    ):
        """
        Creates an alert schedule in Databricks
        """

        run_as_obj = self.create_job_run_as(run_as)

        tags_clone = dict()
        if tags:
            tags_clone.update(tags)
        if alert.query.tags:
            tags_clone.update(alert.query.tags)
        tags_clone["type"] = "alert"
        tags_clone["alert_id"] = alert_id
        tags_clone["destination_id"] = destination_id
        tags_clone["warehouse_id"] = warehouse_id
        tags_clone["migrated_from_redash"] = "true"

        return self.client.jobs.create(
            name=f"Alert `{alert.name}` schedule",
            description=f"Schedule for alert `{alert.name}` ({alert_id}) with destination `{destination_id}`",
            schedule=self._create_cron_schedule(alert.schedule),
            run_as=run_as_obj,
            tags=tags_clone,
            tasks=[
                Task(
                    task_key="alert",
                    sql_task=SqlTask(
                        alert=SqlTaskAlert(
                            alert_id=alert_id,
                            subscriptions=[
                                SqlTaskSubscription(destination_id=destination_id)
                            ],
                        ),
                        warehouse_id=warehouse_id,
                    ),
                )
            ],
        )

    def _create_cron_schedule(self, schedule: dict) -> CronSchedule:
        """
        Creates a cron schedule from Redash schedule
        """
        if schedule["interval"]:
            quarts_expression = self._build_quartz_expression(schedule["interval"])
            return CronSchedule(
                quartz_cron_expression=quarts_expression, timezone_id="UTC"
            )
        raise ValueError("Only interval-based schedules are supported")

    def _build_quartz_expression(self, interval: int) -> str:
        """
        Builds a quartz expression from an interval
        """
        if interval < 60:
            seconds = f"*/{interval}"
            minutes = "*"
            hours = "*"
        elif interval < 3600:
            seconds = f"{interval % 60}"
            minutes = f"*/{interval // 60}"
            hours = "*"
        elif interval < 86400:
            seconds = f"{interval % 60}"
            minutes = f"{(interval // 60) % 60}"
            hours = f"*/{(interval // 60) // 60}"
        else:
            raise ValueError("Interval is too large")

        return f"{seconds} {minutes} {hours} ? * * *"

    def get_path_object_id(self, path: str) -> int:
        """
        Check if a path exists, it is a directory, and return its ID

        :param path: path to check. Ex: /Users/user@something.com/folder/
        :return: ID of the path. If you want to reference this in the API, you need to prepend `folders/` to it.
        """
        status = self.client.workspace.get_status(path)
        if not status:
            raise ValueError(f"Path `{path}`doesn't exist")
        if not status.object_type == ObjectType.DIRECTORY:
            raise ValueError(f"Path `{path}` is not a directory")
        return status.object_id

    def create_job_run_as(self, run_as: str | None = None):
        """
        Creates a job run as object
        """
        if run_as:
            if "@" in run_as:
                return JobRunAs(user_name=run_as)
            else:
                return JobRunAs(service_principal_name=run_as)
        else:
            return None

    def create_directory(self, path: str) -> int:
        """
        Creates a directory in the workspace
        """
        self.client.workspace.mkdirs(path)
        return self.get_path_object_id(path)


    def create_dashboard_ex(self, dashboard: Dashboard, target_folder: str, run_as_role: str = "viewer", tags: list[str] = None, is_favorite: bool = False, dashboard_filters_enabled: bool = True) -> str:
        """
        Create a Databricks dashboard using a Redash dashboard object.

        Args:
            dashboard (Any): Dashboard object.
            target_folder (str): ID or path of the parent folder where the dashboard will be created. We will create a
                                 folder for dashboard
            tags (list, optional): List of tags for the dashboard.
            is_favorite (bool, optional): Set to True if the dashboard should be marked as a favorite.
            run_as_role (str, optional): Role under which the dashboard will run (e.g., "viewer").
            dashboard_filters_enabled (bool, optional): Set to True to enable dashboard filters.

        Returns:
            str: dashboard id from the create_dashboard API.

        Raises:
            ApiException: If there is an error calling the Databricks API.
        """

        name_slug = dashboard.name.replace(" ", "_").lower()
        dashboard_folder = f"{target_folder}/{name_slug}"
        dashboard_folder_id = self.create_directory(dashboard_folder)
        dashboard_queries_folder = f"{dashboard_folder}/queries"
        dashboard_queries_folder_id = self.create_directory(dashboard_queries_folder)

        # Create the dashboard in the draft state
        created_dashboard = self.client.dashboards.create(
            name=dashboard.name,
            parent=f"folders/{dashboard_folder_id}",
            tags=["migrated_from_redash", "original_id:" + str(dashboard.id), *dashboard.tags],
            dashboard_filters_enabled=dashboard.dashboard_filters_enabled,
        )

        queries_to_create = {w.query.id: w.query for w in dashboard.widgets if w.query}.values()
        query_ids = {q.id: self.create_query(q, f"folders/{dashboard_queries_folder_id}") for q in queries_to_create}
        for widget in dashboard.widgets:
            if not widget.query:
                self.create_text_widget(
                    dashboard_id=created_dashboard.id,
                    widget_options=widget.options,
                    text=widget.text,
                    width=widget.width,
                )
            else:
                self.create_widget(
                    dashboard_id=created_dashboard.id,
                    visualization_id=query_ids[widget.query.id][1][widget.visualization.id],
                    widget_options=widget.options,
                    text=widget.text,
                    width=widget.width,
                    title=widget.visualization.name,
                )

    def create_query_ex(self, query: Query, target_folder: str, should_create_folder: bool = None) -> (str, dict[int, str]):
        """
        Given a Query model, creates a Databricks query at the target location.

        If the query depends on other queries (see https://docs.databricks.com/en/sql/user/queries/query-parameters.html#query-based-dropdown-list),
        those dependencies are created first.

        Also, caches mapping of migrated queries to enable re-use
        """

        if should_create_folder is None:
            should_create_folder = False

        if not target_folder.startswith("folders/"):
            if should_create_folder:
                query_slug = query.name.replace(" ", "_").lower()
                target_folder = f"{target_folder}/{query_slug}"
                self.create_directory(target_folder)
            target_folder = f"folders/{self.get_path_object_id(target_folder)}"

        return self.create_query(query, target_folder)
