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

    def test_collect_data_by_submission_uuid_returns_correctly(self):
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
        entity_dict = self.data_collector.collect_data_by_submission_uuid(project_uuid)

        # then
        self._assert_all_entities_are_created(entity_dict, expected_json)
        self._assert_entities_have_correct_inputs(entity_dict)

    def _assert_all_entities_are_created(self, entity_dict, expected_json):
        expected_content_list = [entity['content'] for entity in expected_json]
        actual_content_list = [entity.content for entity in entity_dict.values()]
        self.assertCountEqual(expected_content_list, actual_content_list)

    def _assert_entities_have_correct_inputs(self, entity_dict):
        specimen = entity_dict['6197380b2807a377aad3a303']
        donor = entity_dict['6197380b2807a377aad3a302']
        process = entity_dict['6197380b2807a377aad3a30c']
        protocols = [entity_dict['6197380b2807a377aad3a307']]

        self.assertEqual(specimen.inputs, [donor])
        self.assertEqual(specimen.process, process)
        self.assertEqual(specimen.protocols, protocols)
        self.assertCountEqual([input.id for input in specimen.inputs], [donor.id], 'The specimen has no donor input.')
        self.assertEqual(specimen.process.id, process.id,
                         'The process which links the specimen to the donor is missing.')
        self.assertEqual([protocol.id for protocol in specimen.protocols], [protocol.id for protocol in protocols],
                         'The protocols for the process which links the specimen to the donor are incorrect.')

        cell_suspension = entity_dict['6197380b2807a377aad3a304']
        file = entity_dict['6197380b2807a377aad3a306']
        assay_process = entity_dict['6197380b2807a377aad3a30e']
        assay_process_protocols = [entity_dict['6197380b2807a377aad3a30a'], entity_dict['6197380b2807a377aad3a30b']]

        self.assertCountEqual([input.id for input in file.inputs], [cell_suspension.id],
                              'The sequencing file has no cell suspension input.')
        self.assertEqual(file.process.id, assay_process.id,
                         'The process which links the file to the cell suspension is missing.')
        self.assertEqual([protocol.id for protocol in file.protocols],
                         [protocol.id for protocol in assay_process_protocols],
                         'The protocols for the process which links the file to the cell suspension is incorrect.')

    def _mock_ingest_api(self, project):
        self.mock_ingest_api.get_submission_by_uuid.return_value = project['submission']
        self.mock_ingest_api.get_related_project.return_value = project['project']
        response =  MagicMock()
        response.json.return_value = project['linking_map']
        self.mock_ingest_api.get.return_value = response
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
        with open(self.resources_dir + '/linking-map.json') as file:
            linking_map = json.load(file)

        return {
            'submission': mock_submission_json,
            'project': mock_project_json,
            'biomaterials': mock_biomaterials_json,
            'processes': mock_processes_json,
            'protocols': mock_protocols_json,
            'files': mock_files_json,
            'linking_map': linking_map
        }


if __name__ == '__main__':
    unittest.main()
