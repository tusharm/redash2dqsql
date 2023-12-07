import uuid

from databricks.sdk import WorkspaceClient

from extract import Query


class DBXClient:
    def __init__(self, url, token):
        self.client = WorkspaceClient(host=url, token=token)
        self.warehouse_id = self.client.data_sources.list()[0].id

    def create_query(self, query: Query):
        return self.client.queries.create(name=f'tushar-job-estimator-{uuid.uuid4()}',
                                          data_source_id=self.warehouse_id,
                                          description="Test query created by Tushar",
                                          query=query.query_string,
                                          parent="/tushar",
                                          options=query.options)
