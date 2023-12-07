## redash2dbsql

A tool to migrate Redash dashboards/queries to Databricks SQL

### Features

1. Fetches queries and dashboards by tags
2. Builds dependencies between the queries (see [this](https://docs.databricks.com/en/sql/user/queries/query-parameters.html#query-based-dropdown-list))
3. Uses [sqlglot](https://sqlglot.com/sqlglot.html) to convert to Databricks format

### Issues

1. Databricks SDK doesn't support all Query parameter types (see https://github.com/databricks/databricks-sdk-py/issues/475)
2. Resources are not getting created at the target location, as defined by `parent` parameter in the [API](https://docs.databricks.com/api/workspace/queries/create). They always get created in the user's home directory.