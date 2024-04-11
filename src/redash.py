from __future__ import annotations

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


@dataclass
class Alert:
    id: int
    name: str
    query: Query
    schedule: dict | None
    options: dict
    rearm: int | None


class RedashClient:
    def __init__(self, url, api_key):
        self.redash = Redash(url, api_key)

    def dashboards(self, tags=None):
        """
        Returns a list of dashboards, optionally filtered by tags
        """
        dashboards = self.redash.dashboards(tags=tags)
        return map(lambda x: self.redash.dashboard(x['id']), dashboards['results'])

    def get_dashboard(self, id):
        """
        Returns a dashboard, by id
        """
        return self.redash.get_dashboard(id)

    def queries(self, tags=None, query_id: int | None = None) -> [Query]:
        """
        Returns a list of queries, optionally filtered by tags
        """
        if query_id:
            query_objs = [self.redash.get_query(query_id)]
        else:
            query_objs = self.redash.queries(tags=tags)['results']
        return [
            self._build_query_model(q)
            for q in query_objs
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

    def alerts(self, tags: list[str] = None, alert_id: int = None) -> list[Alert]:
        """
        Returns a list of alerts
        """
        if alert_id:
            alerts = [self.redash.get_alert(alert_id)]
        else:
            alerts = self.redash.alerts()
        if tags:  # alerts it-self don't have tags, but queries do
            alerts_filtered = [a for a in alerts if set(tags).issubset(a["query"]["tags"])]
        else:
            alerts_filtered = alerts
        return [self._build_alert_model(a) for a in alerts_filtered]

    def _build_alert_model(self, alert_obj) -> Alert:
        return Alert(
            id=alert_obj['id'],
            name=alert_obj['name'],
            query=self._build_query_model(alert_obj['query']),
            options=alert_obj['options'],
            rearm=alert_obj.get('rearm'),
            schedule=alert_obj["query"].get("schedule"),
        )
