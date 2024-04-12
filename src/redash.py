from __future__ import annotations

import enum
from dataclasses import dataclass, field

from redash_toolbelt import Redash


class VisualizationType(enum.Enum):
    WORD_CLOUD = "WORD_CLOUD"
    TABLE = "TABLE"
    COHORT = "COHORT"
    COUNTER = "COUNTER"
    SANKEY = "SANKEY"
    FUNNEL = "FUNNEL"
    MAP = "MAP"
    PIVOT = "PIVOT"
    CHART = "CHART"
    DETAILS = "DETAILS"


@dataclass
class Visualization:
    id: int
    type: VisualizationType
    name: str
    description: str
    options: dict


@dataclass
class Query:
    id: int
    name: str
    query_string: str
    options: dict = field(default_factory=dict)
    tags: [] = field(default_factory=list)
    depends_on: [] = field(default_factory=list)
    visualizations: list[Visualization] = field(default_factory=list)

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


@dataclass
class Widget:
    id: int
    text: str | None
    query: Query | None
    visualization: Visualization | None
    options: dict | None = None
    width: int | None = None


@dataclass
class Dashboard:
    id: int
    name: str
    slug: str | None = None
    widgets: list[Widget] | None = None
    dashboard_filters_enabled: bool | None = None
    layout: list | None = None
    tags: list[str] | None = None


class RedashClient:
    def __init__(self, url, api_key):
        self.redash = Redash(url, api_key)

    def dashboards(self, tags=None):
        """
        Returns a list of dashboards, optionally filtered by tags
        """
        dashboards = self.redash.dashboards(tags=tags)
        return [self.get_dashboard(d['id']) for d in dashboards['results']]

    def get_dashboard(self, id):
        """
        Returns a dashboard, by id
        """
        return self._build_dashboard_model(self.redash.get_dashboard(id))

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

    def query_for_widget(self, widget) -> Query | None:
        """
        Returns a query linked to a given widget, as a Query object
        """
        if 'visualization' in widget and 'query' in widget['visualization']:
            return self.queries(query_id=widget['visualization']['query']['id'])[0]
        return None

    def _build_query_model(self, query_obj) -> Query:
        query = Query(
            id=query_obj['id'],
            name=query_obj['name'],
            query_string=query_obj['query'],
            options=query_obj['options'],
            tags=query_obj['tags'],
            depends_on=self._depends_on_queries(query_obj)
        )
        if 'visualizations' in query_obj:
            query.visualizations.extend([
                self._build_visualization_model(v)
                for v in query_obj['visualizations']
            ])
        return query

    def _build_visualization_model(self, visualization_obj) -> Visualization:
        return Visualization(
            id=visualization_obj['id'],
            type=VisualizationType(visualization_obj['type']),
            name=visualization_obj['name'],
            description=visualization_obj['description'],
            options=visualization_obj['options']
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

    def _build_dashboard_model(self, dashboard_obj) -> Dashboard:

        return Dashboard(
            id=dashboard_obj['id'],
            name=dashboard_obj['name'],
            slug=dashboard_obj['slug'],
            widgets=[
                self._build_widget_model(w)
                for w in dashboard_obj['widgets']
            ],
            dashboard_filters_enabled=dashboard_obj.get('dashboard_filters_enabled'),
            layout=dashboard_obj.get('layout'),
            tags=dashboard_obj.get('tags')
        )

    def _build_widget_model(self, widget_obj) -> Widget:
        options = widget_obj.get('options')
        if 'visualization' in widget_obj:
            query = self.query_for_widget(widget_obj)
            visualization = {v.id: v for v in query.visualizations}.get(widget_obj['visualization']['id'])
            return Widget(
                id=widget_obj['id'],
                text=widget_obj.get('text'),
                query=query,
                visualization=visualization,
                options=options,
                width=widget_obj.get('width')
            )
        return Widget(
            id=widget_obj['id'],
            text=widget_obj.get('text'),
            query=None,
            visualization=None,
            options=options,
            width=widget_obj.get('width')
        )


