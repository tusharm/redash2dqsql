import re

import sqlglot.errors
from sqlglot import transpile, parse_one, exp

from redash import Query
from hlog import LOGGER


TARGET_DIALECT = 'databricks'


def org_specific_pre_transformations(query: Query, from_dialect='presto'):
    """
    Apply org specific pre-transpiled transformations to the query
    """

    identified_tables = set()

    def get_table_name(table, db):
        tbl_name = f"lakehouse_production.kafka_cdc.{db}_{table}"
        identified_tables.add(tbl_name)
        return tbl_name

    from_pat = re.compile(r'FROM\s+`?(\w+)`?\.?`?(\w+)?`?', re.IGNORECASE)
    join_pat = re.compile(r'JOIN\s+`?(\w+)`?\.?`?(\w+)?`?', re.IGNORECASE)

    if from_dialect == 'mysql':
        tqs = []
        current_db = 'hip'
        for l in query.query_string.split('\n'):
            if len(l.strip()) == 0:
                continue
            if l.strip().lower().startswith('USE'):
                current_db = l.split()[1].strip().strip(';')
                continue
            if from_pat.search(l):
                table = from_pat.search(l).group(2)
                db = from_pat.search(l).group(1)
                if table is None:
                    table = db
                    db = current_db
                tqs.append(' '.join([l[:from_pat.search(l).start()], 'FROM', get_table_name(table, db), l[from_pat.search(l).end():]]))
            elif join_pat.search(l):
                table = join_pat.search(l).group(2)
                db = join_pat.search(l).group(1)
                if table is None:
                    table = db
                    db = current_db
                tqs.append(' '.join([l[:join_pat.search(l).start()], 'JOIN', get_table_name(table, db), l[join_pat.search(l).end():]]))
            else:
                tqs.append(l)
        query.query_string = '\n'.join(tqs)
        if len(identified_tables) > 0:
            with open('identified_tables.txt', 'w') as f:
                f.write('\n'.join(identified_tables))

    return query


def org_specific_post_transformations(query: Query, from_dialect='presto'):
    """
    Apply org specific post-transpiled transformations to the query
    """
    return query


def transform_query(query: Query, from_dialect=None):
    """
    Transforms the query from the given dialect to Databricks dialect.
    Also, applies post-processing steps on the transformed results:
        1. qualifies table names with catalog
        2. fixes query params, messed up by sqlglot
    """
    for q in query.depends_on:
        transform_query(q, from_dialect)

    if from_dialect is None:
        from_dialect = query.source.dialect
    if from_dialect is None:
        from_dialect = 'presto'

    org_specific_pre_transformations(query, from_dialect=from_dialect)

    try:

        transpiled = transpile(query.query_string.strip(), read=from_dialect, write=TARGET_DIALECT, pretty=True,
                               error_level=sqlglot.errors.ErrorLevel.IGNORE)

        # we will only have one query
        result = transpiled[0]
    except sqlglot.errors.SqlglotError as e:
        LOGGER.error(f"Error transpiling query: {query.name}")
        result = query.query_string

    # result = qualify_tables_with_catalog(result, dialect=TARGET_DIALECT, catalog='hive_metastore')
    result = fix_query_params(result, query.params)

    query.query_string = result
    org_specific_post_transformations(query, from_dialect=from_dialect)


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
