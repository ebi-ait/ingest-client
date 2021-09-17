import json
import os
import unittest
from unittest.mock import MagicMock

from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector


class DataCollectorTest(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.mock_ingest_api = MagicMock(spec=IngestApi)
        self.data_collector = DataCollector(self.mock_ingest_api)
        self.script_dir = os.path.dirname(__file__)
        self.parent_dir = os.path.split(self.script_dir)[0]
        self.resources_dir = os.path.join(self.parent_dir, 'resources')

    def test_collected_project_and_biomaterials_data_by_submission_uuid_returns_correctly(self):
        # given
        project = self._make_project_data()
        self._mock_ingest_api(project)

        expected_json = [project['project']] + \
                        project['biomaterials'] + \
                        project['processes'] + \
                        project['protocols'] + \
                        project['files']

        # when
        project_uuid = '1234'
        result_json = self.data_collector.collect_data_by_submission_uuid(project_uuid)

        # then
        self.assertEqual(result_json, expected_json)

    def _mock_ingest_api(self, project):
        self.mock_ingest_api.get_submission_by_uuid.return_value = project['submission']
        self.mock_ingest_api.get_related_project.return_value = project['project']
        self.mock_ingest_api.get_related_entities.side_effect = \
            [
                iter(project['biomaterials']),
                iter(project['processes']),
                iter(project['protocols']),
                iter(project['files'])
            ]

    def _make_project_data(self):
        with open(self.resources_dir + '/mock_submission.json') as file:
            mock_submission_json = json.load(file)
        with open(self.resources_dir + '/mock_project.json') as file:
            mock_project_json = json.load(file)
        with open(self.resources_dir + '/mock_biomaterials.json') as file:
            mock_biomaterials_json = json.load(file)
        with open(self.resources_dir + '/mock_processes.json') as file:
            mock_processes_json = json.load(file)
        with open(self.resources_dir + '/mock_protocols.json') as file:
            mock_protocols_json = json.load(file)
        with open(self.resources_dir + '/mock_files.json') as file:
            mock_files_json = json.load(file)

        return {
            'submission': mock_submission_json,
            'project': mock_project_json,
            'biomaterials': mock_biomaterials_json,
            'processes': mock_processes_json,
            'protocols': mock_protocols_json,
            'files': mock_files_json
        }


if __name__ == '__main__':
    unittest.main()
