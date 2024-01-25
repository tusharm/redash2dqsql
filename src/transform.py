import sqlglot.errors
from sqlglot import transpile, parse_one, exp

from redash import Query

TARGET_DIALECT = 'databricks'


def transform_query(query: Query, from_dialect='presto'):
    """
    Transforms the query from the given dialect to Databricks dialect.
    Also, applies post-processing steps on the transformed results:
        1. qualifies table names with catalog
        2. fixes query params, messed up by sqlglot
    """
    for q in query.depends_on:
        transform_query(q)

    transpiled = transpile(query.query_string, read=from_dialect, write=TARGET_DIALECT, pretty=True,
                           error_level=sqlglot.errors.ErrorLevel.IGNORE)

    # we will only have one query
    result = transpiled[0]

    # result = qualify_tables_with_catalog(result, dialect=TARGET_DIALECT, catalog='hive_metastore')
    result = fix_query_params(result, query.params)

    query.query_string = result


def fix_query_params(query: str, params) -> str:
    """
    Expectedly, sqlglot doesn't handle Redash query params properly eg
    `{{param}}` is transpiled to `STRUCT(STRUCT(param))`
    This function fixes this.
    """
    result = query
    for param in params:
        result = result.replace(f"STRUCT(STRUCT({param}))", f"{{{{{param}}}}}")
    return result


def qualify_tables_with_catalog(query: str, dialect, catalog) -> str:
    """
    Prefix table names with the catalog name
    """
    expression_tree = parse_one(query, read=dialect)

    def transformer(node):
        if isinstance(node, exp.Table) and len(node.parts) > 1:
            return exp.Table(this=node.name, db=node.db, catalog=catalog, alias=node.alias)
        return node

    transformed_tree = expression_tree.transform(transformer)
    return transformed_tree.sql(pretty=True, dialect=dialect)
