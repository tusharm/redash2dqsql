## redash2dbsql

Tool to migrate Redash dashboards to Databricks SQL

### 

1. Fetches queries and dashboards by tags
2. Builds dependencies between the queries
3. Uses `sqlglot` to convert to Databricks format
   1. Ensures query parameters are not messed up
   2. Fixes schema names

### Issues

1. 
2. 