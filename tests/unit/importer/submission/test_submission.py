from unittest import TestCase

from mock import MagicMock

import ingest.api.ingestapi
from ingest.importer.submission.entity import Entity
from ingest.importer.submission.submission import Submission


class SubmissionTest(TestCase):

    def test_new_submission(self):
        # given
        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = MagicMock(name='mock_ingest_api')

        submission = Submission(mock_ingest_api, submission_url='submission_url')
        submission_url = submission.get_submission_url()

        self.assertEqual('submission_url', submission_url)

    def test_add_metadata(self):
        mock_ingest_api = MagicMock(name='mock_ingest_api')
        submission = Submission(mock_ingest_api, submission_url='url')
        entity = Entity(entity_id='id', entity_type='biomaterial', content={})
        entity = submission.add_entity(entity)
        self.assertEqual(entity, submission.get_entity('biomaterial', 'id'))

    def test_define_manifest(self):
        # expect:
        self._do_test_define_manifest(32)
        self._do_test_define_manifest(899)
        self._do_test_define_manifest(45219)

    def _do_test_define_manifest(self, total_count):
        # given:
        ingest_api = MagicMock('mock_ingest_api')
        ingest_api.create_submission_manifest = MagicMock()
        url = 'http://core.sample.com/submission/8fd733'
        submission = Submission(ingest_api, url)

        count_per_entity = {
            'biomaterial': 5,
            'project': 1,
            'protocol': 5,
            'file': 10,
            'process': 5
        }
        # and:
        entity_map = MagicMock('entity_map')
        entity_map.count_total = MagicMock(return_value=total_count)
        entity_map.count_entities_of_type = lambda entity: count_per_entity.get(entity)
        entity_map.count_links = MagicMock(return_value=total_count)

        # when:
        submission.define_manifest(entity_map)

        # then:
        ingest_api.create_submission_manifest.assert_called()
        ingest_api_args, __ = ingest_api.create_submission_manifest.call_args
        self.assertEqual(2, len(ingest_api_args))
        self.assertEqual(url, ingest_api_args[0])

        # and:
        raw_json = ingest_api_args[1]
        submitted_json = raw_json
        self.assertEqual(total_count, submitted_json['totalCount'])
        self.assertEqual(count_per_entity['biomaterial'], submitted_json['expectedBiomaterials'])
        self.assertEqual(count_per_entity['process'], submitted_json['expectedProcesses'])
        self.assertEqual(count_per_entity['file'], submitted_json['expectedFiles'])
        self.assertEqual(count_per_entity['protocol'], submitted_json['expectedProtocols'])
        self.assertEqual(count_per_entity['project'], submitted_json['expectedProjects'])
        self.assertEqual(total_count, submitted_json['expectedLinks'])


def _create_spreadsheet_json():
    spreadsheet_json = {
        'project': {
            'dummy-project-id': {
                'content': {
                    'key': 'project_1'
                }
            }
        },
        'biomaterial': {
            'biomaterial_id_1': {
                'content': {
                    'key': 'biomaterial_1'
                }
            },
            'biomaterial_id_2': {
                'content': {
                    'key': 'biomaterial_2'
                },
                'links_by_entity': {
                    'biomaterial': ['biomaterial_id_1'],
                    'process': ['process_id_1']
                }
            },
            'biomaterial_id_3': {
                'content': {
                    'key': 'biomaterial_3'
                },
                'links_by_entity': {
                    'biomaterial': ['biomaterial_id_2'],
                    'process': ['process_id_2']
                }
            },
            'biomaterial_id_4': {
                'content': {
                    'key': 'biomaterial_3'
                },
                'links_by_entity': {
                    'biomaterial': ['biomaterial_id_2'],
                    'process': ['process_id_2']
                },
                'external_links_by_entity': {
                    'biomaterial': ['biomaterial_uuid']
                },

            },
        },
        'file': {
            'file_id_1': {
                'content': {
                    'file_core': {
                        'file_name': 'file_name'
                    }
                },
                'links_by_entity': {
                    'biomaterial': ['biomaterial_id_3']
                }
            }
        },
        'protocol': {
            'protocol_id_1': {
                'content': {
                    'key': 'protocol_1'
                }
            }
        }
    }

    return spreadsheet_json
