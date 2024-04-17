## redash2dbsql

A tool to migrate Redash dashboards/queries to Databricks SQL

### Features

1. Fetches queries and dashboards by tags
2. Builds dependencies between the queries (see [this](https://docs.databricks.com/en/sql/user/queries/query-parameters.html#query-based-dropdown-list))
3. Uses [sqlglot](https://sqlglot.com/sqlglot.html) to convert to Databricks format

### Issues

1. Databricks SDK doesn't support all Query parameter types (see https://github.com/databricks/databricks-sdk-py/issues/475)
2. Resources are not getting created at the target location, as defined by `parent` parameter in the [API](https://docs.databricks.com/api/workspace/queries/create). They always get created in the user's home directory.


### Installation

This application is a python package and can be installed using pip.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```


### CLI Usage

```bash
source .venv/bin/activate
python src/cli.py --help
```

```bash
Usage: cli.py [OPTIONS] COMMAND [ARGS]...

Options:
  --version                Show the version and exit.
  --redash-url TEXT        Redash URL
  --redash-api-key TEXT    Redash API Key
  --databricks-host TEXT   Redash API Key
  --databricks-token TEXT  Redash API Key
  --help                   Show this message and exit.

Commands:
  alerts
  dashboards
  queries
```
You need to provide the Redash URL and API Key and Databricks host and token as command line options 
or environment variables.

If you are providing them as environment variables, you can use following dotenv file.
```dotenv
REDASH_URL=https:/[YOUR REDASH URL]
REDASH_API_KEY=[YOUR API KEY]
DATABRICKS_HOST=https://[YOUR WORKSPACE URL].cloud.databricks.com
DATABRICKS_TOKEN=[YOUR TOKEN]
```

