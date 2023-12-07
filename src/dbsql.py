from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryOptions, Parameter, ParameterType

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
