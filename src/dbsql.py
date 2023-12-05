from sqlglot import transpile, parse_one, exp

from redash import Query


def convert_from(query: Query, dialect='presto'):
    transpiled = transpile(query.query_string, read=dialect, write="databricks", pretty=True)

    # we will only have one query
    result = transpiled[0]

    # TODO: this is a hack. need to check if this is true only for PRESTO dialect
    result = fix_schema_names(result)
    result = fix_query_params(result, query.params)

    return result


def fix_query_params(query: str, params):
    result = query
    for param in params:
        result = result.replace(f"STRUCT(STRUCT({param}))", f"{{{{{param}}}}}")
    return result


def fix_schema_names(query: str, dialect='databricks'):
    # Parse the SQL statement
    expression_tree = parse_one(query, read=dialect)

    def transformer(node):
        if isinstance(node, exp.Table) and len(node.parts) > 1:
            return exp.Table(this=node.name, db=node.db, catalog="hive_metastore", alias=node.alias)
        return node

    transformed_tree = expression_tree.transform(transformer)
    return transformed_tree.sql(pretty=True, dialect=dialect)
