from dataclasses import dataclass

from redash_toolbelt import Redash


@dataclass
class Query:
    id: int
    name: str
    query_string: str
    options: dict
    tags: []
    depends_on: []

    @property
    def params(self):
        parameters = self.options.get('parameters', [])
        return [p['name'] for p in parameters]


class RedashClient:
    def __init__(self, url, api_key):
        self.redash = Redash(url, api_key)

    def dashboards(self, tags=None):
        """
        Returns a list of dashboards, optionally filtered by tags
        """
        dashboards = self.redash.dashboards(tags=tags)
        return map(lambda x: self.redash.dashboard(x['id']), dashboards['results'])

    def queries(self, tags=None) -> [Query]:
        """
        Returns a list of queries, optionally filtered by tags
        """
        query_objs = self.redash.queries(tags=tags)
        return [
            self._build_query_model(q)
            for q in query_objs['results']
        ]

    def queries_for(self, dashboard) -> [Query]:
        """
        Returns queries linked to a given dashboard, as a list of Query objects
        """
        return [
            self._build_query_model(w['visualization']['query'])
            for w in dashboard['widgets']
            if 'visualization' in w
        ]

    def _build_query_model(self, query_obj) -> Query:
        return Query(
            id=query_obj['id'],
            name=query_obj['name'],
            query_string=query_obj['query'],
            options=query_obj['options'],
            tags=query_obj['tags'],
            depends_on=self._depends_on_queries(query_obj)
        )

    def _depends_on_queries(self, query) -> [Query]:
        """
        Redash queries can have parameters that are based on other queries
        (see https://redash.io/help/user-guide/querying/query-parameters#Dropdown-Lists).
        This method generates a list of Query objects that the given query depends on.
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