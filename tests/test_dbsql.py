from unittest import TestCase
from unittest.mock import MagicMock

from databricks.sdk.service.workspace import ObjectType


class TestDBXClient(TestCase):

    def setUp(self):
        import dbsql
        dbsql.WorkspaceClient = MagicMock()
        self.subject = dbsql.DBXClient('host', 'token')

    def test_create_job_run_as(self):
        result = self.subject.create_job_run_as('user@something.com')
        self.assertEqual(result.user_name, 'user@something.com')

        result = self.subject.create_job_run_as('1111-1111-1111-1111')
        self.assertEqual(result.service_principal_name, '1111-1111-1111-1111')

    def test_get_path_object_id(self):
        self.subject.client.workspace.get_status.return_value = MagicMock(object_type=ObjectType.DIRECTORY, object_id=1234)
        result = self.subject.get_path_object_id('/some/path/')
        self.assertEqual(result, 1234)

        self.subject.client.workspace.get_status.return_value = None
        with self.assertRaisesRegex(ValueError, "Path `/some/path/`doesn't exist"):
            self.subject.get_path_object_id('/some/path/')

        self.subject.client.workspace.get_status.return_value = MagicMock(object_type=ObjectType.NOTEBOOK, object_id=1234)
        with self.assertRaisesRegex(ValueError, "Path `/some/path/` is not a directory"):
            self.subject.get_path_object_id('/some/path/')
