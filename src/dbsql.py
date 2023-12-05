from sqlglot import transpile

from redash import Query


def convert_from(query: Query, dialect='presto'):
    transpiled = transpile(query.query_string, read=dialect, write="databricks", pretty=True)

    # we will only have one query
    result = transpiled[0]

    # TODO: this is a hack. need to check if this is true only for PRESTO dialect
    for param in query.params:
        result = result.replace(f"STRUCT(STRUCT({param}))", f"{{{{{param}}}}}")

    return result
