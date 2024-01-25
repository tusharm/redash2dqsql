import os
from unittest import TestCase
from unittest.mock import MagicMock

from redash import RedashClient


class TestRedashClient(TestCase):

    sample_alerts = [
        {'id': 179, 'name': '[Data Platform] Redash is working?: c < 1',
         'options': {'op': '<', 'value': 1, 'muted': False, 'column': 'c'}, 'state': 'ok',
         'query': {'id': 3804, 'latest_query_data_id': 8340040, 'name': '[Data Platform] Redash is working?',
                   'description': None, 'query': 'select 1 as c', 'query_hash': '2dd3931aa376502f38c37cdab6fe597d',
                   'schedule': {'interval': 300, 'time': None, 'day_of_week': None, 'until': None},
                   'is_archived': False, 'is_draft': False,
                   'updated_at': '2023-10-22T23:39:04.772Z', 'created_at': '2023-10-22T23:36:49.884Z',
                   'data_source_id': 1, 'options': {'apply_auto_limit': False, 'parameters': []}, 'version': 1,
                   'tags': [], 'is_safe': True
                   }
         }
    ]

    def setUp(self):
        self.api_endpoint = os.getenv("REDASH_API_ENDPOINT", "https://redash.example.example")
        self.api_key = os.getenv("REDASH_API_KEY", "some_api_key")
        self.client = RedashClient(self.api_endpoint, self.api_key)
        self.client.redash = MagicMock()

    def test_alerts(self):
        # without tags
        self.client.redash.alerts.return_value = self.sample_alerts
        results = self.client.alerts()
        self.client.redash.alerts.assert_called_once()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, 179)
        self.assertEqual(results[0].name, '[Data Platform] Redash is working?: c < 1')
        self.assertEqual(results[0].query.query_string, 'select 1 as c')
        self.assertEqual(results[0].query.name, '[Data Platform] Redash is working?')
        self.assertEqual(results[0].schedule['interval'], 300)
