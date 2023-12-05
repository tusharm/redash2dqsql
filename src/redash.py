from dataclasses import dataclass

from redash_toolbelt import Redash


class Client:
    def __init__(self, redash: Redash):
        self.redash = redash

    def get_dashboards(self, tags=None):
        dashboards = self.redash.dashboards(tags=tags)
        return map(lambda x: self.redash.dashboard(x['id']), dashboards['results'])


@dataclass
class Query:
    query_string: str
    params: [str]


def get_queries(dashboard) -> [Query]:
    def params(query):
        return [p['name'] for p in query['options']['parameters']]

    return [
        Query(w['visualization']['query']['query'], params(w['visualization']['query']))
        for w in dashboard['widgets'] if 'visualization' in w
    ]
