from dataclasses import dataclass

from redash_toolbelt import Redash


@dataclass
class Query:
    name: str
    id: int
    query_string: str
    options: dict
    tags: []
    depends_on = []

    @property
    def params(self):
        parameters = self.options.get('parameters', [])
        return [p['name'] for p in parameters]

    def update_query_based_parameter(self, id: int, new_id: int):
        for param in self.options.get('parameters', []):
            if param.get('queryId') and param['queryId'] == id:
                param['queryId'] = new_id


class RedashClient:
    def __init__(self, url, api_key):
        self.redash = Redash(url, api_key)

    def dashboards(self, tags=None):
        dashboards = self.redash.dashboards(tags=tags)
        return map(lambda x: self.redash.dashboard(x['id']), dashboards['results'])

    def queries(self, tags=None) -> [Query]:
        query_objs = self.redash.queries(tags=tags)
        return [
            self._build_query_model(q)
            for q in query_objs['results']
        ]

    def queries_for(self, dashboard) -> [Query]:
        queries = []
        for w in dashboard['widgets']:
            if 'visualization' in w:
                redash_query = w['visualization']['query']
                queries.append(self._build_query_model(redash_query))
        return queries

    def _build_query_model(self, redash_query) -> Query:
        query = Query(
            id=redash_query['id'],
            name=redash_query['name'],
            query_string=redash_query['query'],
            options=redash_query['options'],
            tags=redash_query['tags']
        )

        query.depends_on = self._depends_on_queries(redash_query)
        return query

    def _depends_on_queries(self, query) -> [Query]:
        """
        Generates a list of Query objects that the given query depends on.
        """
        params = query['options'].get('parameters')
        if not params:
            return []

        def query_id_exists(x):
            return x.get('queryId') is not None

        return [
            self._build_query_model(self.redash.get_query(p['queryId']))
            for p in filter(query_id_exists, params)
        ]
