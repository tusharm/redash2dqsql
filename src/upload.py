from datetime import datetime

from databricks.sdk import WorkspaceClient

from extract import Query


class DBXClient:
    def __init__(self, url, token):
        self.client = WorkspaceClient(host=url, token=token)
        self.warehouse_id = self.client.data_sources.list()[0].id

        # TODO: ideally this should be external eg a Delta table
        self.cache = dict()

    def create_query(self, query: Query, target_folder: str):
        self._process_dependencies(query, target_folder)

        created = self.client.queries.create(
            name=query.name,
            data_source_id=self.warehouse_id,
            description=f"Migrated from Redash on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, tags: {query.tags}",
            query=query.query_string,
            parent=target_folder,
            options=query.options
        )

        self.store_migrated_id(query.id, created.id)
        return created

    def get_migrated_id(self, redash_query_id: int) -> int:
        """
        Looks up a cache to see if this query has been already migrated
        """

        return self.cache.get(redash_query_id, -1)

    def store_migrated_id(self, redash_id: int, dbx_id: int):
        """
        Update the cache, so we can find the ID later
        """
        self.cache[redash_id] = dbx_id

    def _process_dependencies(self, query, target_folder):
        """
        If this query depends on other queries, we need to migrate them first
        """
        for q in query.depends_on:
            migrated_id = self.get_migrated_id(q.id)
            if migrated_id < 0:
                dep_query = self.create_query(q, target_folder)
                migrated_id = dep_query.id

            query.update_query_based_parameter(q.id, migrated_id)
