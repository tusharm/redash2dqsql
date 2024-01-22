from __future__ import annotations

from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CronSchedule, SqlTask, Task, SqlTaskAlert, SqlTaskSubscription
from databricks.sdk.service.sql import QueryOptions, Parameter, ParameterType, AlertOptions
from databricks.sdk.service.workspace import ObjectType

from redash import Query, Alert


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

    def create_alert(self, alert: Alert, target_folder: str, destination_id: str | None = None, warehouse_id: str | None = None) -> str:
        """
        Given a Redash alert, creates a Databricks alert

        :param alert: Redash alert model
        :param target_folder: target folder to create the alert in
        :param destination_id: optional ID of the destination if schedule is set
        :param warehouse_id: optional ID of the SQL warehouse to refresh query
        """

        target_folder_path = "folders/{0}".format(self._get_path_object_id(target_folder))

        # First migrate the query
        query_id = self.create_query(alert.query, target_folder_path)
        result = self._create_alert_api_call(query_id, alert, target_folder_path)
        if alert.schedule and destination_id and warehouse_id:
            self._create_alert_schedule_api_call(result.id, alert.schedule, destination_id, warehouse_id)
        return result.id

    def _create_alert_api_call(self, query_id: str, alert: Alert, parent_folder: str):
        """
        Creates an alert in Databricks
        """
        return self.client.alerts.create(
            name=alert.name,
            options=AlertOptions.from_dict(alert.options),
            query_id=query_id,
            parent=parent_folder,
            rearm=alert.rearm,
        )

    def _create_alert_schedule_api_call(self, alert_id: str, schedule: dict, destination_id: str, warehouse_id: str):
        """
        Creates an alert schedule in Databricks
        """
        return self.client.jobs.create(
            name=f"Alert {alert_id} schedule",
            schedule=self._create_cron_schedule(schedule),
            tasks=[
                Task(
                    task_key="alert",
                    sql_task=SqlTask(alert=SqlTaskAlert(alert_id=alert_id, subscriptions=[
                        SqlTaskSubscription(destination_id=destination_id)
                    ]), warehouse_id=warehouse_id),
                )
            ],

        )

    def _create_cron_schedule(self, schedule: dict) -> CronSchedule:
        """
        Creates a cron schedule from Redash schedule
        """
        if schedule['interval']:
            quarts_expression = self._build_quartz_expression(schedule['interval'])
            return CronSchedule(
                quartz_cron_expression=quarts_expression,
                timezone_id="UTC"
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
            hours = '*'
        elif interval < 86400:
            seconds = f"{interval % 60}"
            minutes = f"{(interval // 60) % 60}"
            hours = f"*/{(interval // 60) // 60}"
        else:
            raise ValueError("Interval is too large")

        return f"{seconds} {minutes} {hours} ? * * *"

    def _get_path_object_id(self, path: str) -> int:
        """
        Check if a path exists, it is a directory, and return its ID
        """
        status = self.client.workspace.get_status(path)
        if not status:
            raise ValueError(f"Path `{path}`doesn't exist")
        if not status.object_type == ObjectType.DIRECTORY:
            raise ValueError(f"Path `{path}` is not a directory")
        return status.object_id
