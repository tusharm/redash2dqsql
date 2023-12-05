from databricks.sdk import WorkspaceClient


class DBXClient:
    def __init__(self, url, token):
        self.client = WorkspaceClient(host=url, token=token)
