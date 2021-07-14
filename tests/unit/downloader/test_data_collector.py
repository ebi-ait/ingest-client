import json
import unittest
from unittest.mock import MagicMock, Mock

from ingest.api.ingestapi import IngestApi
from ingest.downloader.data_collector import DataCollector


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.mock_ingest_api = MagicMock(spec=IngestApi)
        self.data_collector = DataCollector(self.mock_ingest_api)

    def test_collected_project_and_biomaterials_data_by_submission_uuid_returns_correctly(self):
        #given
        project_uuid = '1234'
        with open('../resources/mock_submission.json') as file:
            mock_submission_json = json.load(file)
        with open('../resources/mock_project.json') as file:
            mock_project_json = json.load(file)
        with open('../resources/mock_biomaterials.json') as file:
            mock_biomaterials_json = json.load(file)

        self.mock_ingest_api.get_submission_by_uuid.return_value = mock_submission_json
        self.mock_ingest_api.get_all.return_value = iter([mock_project_json])
        self.mock_ingest_api.get_related_entities.return_value = iter(mock_biomaterials_json)

        expected_json = [
            mock_project_json,
        ]
        expected_json.extend(mock_biomaterials_json)

        #when
        result_json = self.data_collector.collect_data_by_submission_uuid(project_uuid)

        #then
        self.assertEqual(result_json, expected_json)


if __name__ == '__main__':
    unittest.main()
