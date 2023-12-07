from sqlglot import transpile, parse_one, exp

from extract import Query


def convert_query(query: Query, from_dialect='presto', to_dialect='databricks'):
    for q in query.depends_on:
        convert_query(q, from_dialect=from_dialect)

    transpiled = transpile(query.query_string, read=from_dialect, write=to_dialect, pretty=True)

    # we will only have one query
    result = transpiled[0]

    # TODO: this is a hack. need to check if this is true only for PRESTO dialect
    result = fix_schema_names(result, dialect=to_dialect)
    result = fix_query_params(result, query.params)

    query.query_string = result


def fix_query_params(query: str, params) -> str:
    result = query
    for param in params:
        result = result.replace(f"STRUCT(STRUCT({param}))", f"{{{{{param}}}}}")
    return result


def fix_schema_names(query: str, dialect) -> str:
    # Parse the SQL statement
    expression_tree = parse_one(query, read=dialect)

    def transformer(node):
        if isinstance(node, exp.Table) and len(node.parts) > 1:
            return exp.Table(this=node.name, db=node.db, catalog="hive_metastore", alias=node.alias)
        return node

    transformed_tree = expression_tree.transform(transformer)
    return transformed_tree.sql(pretty=True, dialect=dialect)
