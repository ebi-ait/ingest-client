import json
from unittest import TestCase
from unittest.mock import Mock

from mock import MagicMock, patch, call

import ingest.api.ingestapi
from ingest.importer.submission.entity import Entity
from ingest.importer.submission.entity_linker import EntityLinker
from ingest.importer.submission.entity_map import EntityMap
from ingest.importer.submission.errors import InvalidLinkInSpreadsheet, LinkedEntityNotFound, MultipleProcessesFound
from ingest.importer.submission.ingest_submitter import IngestSubmitter
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
        new_entity_mock_response = {
            'content': {},
            'submissionDate': '2018-05-08T10:17:49.476Z',
            'updateDate': '2018-05-08T10:17:57.254Z',
            'user': 'anonymousUser',
            'lastModifiedUser': 'anonymousUser',
            'uuid': {
                'uuid': '5a36689b-302b-40e4-bef1-837b47f0cb51'
            },
            'validationState': 'Draft',
            '_links': {
                'self': {
                    'href': 'http://api.ingest.dev.data.humancellatlas.org/protocols/5af1794d6a65a50007755b6a'
                },
                'protocol': {
                    'href': 'http://api.ingest.dev.data.humancellatlas.org/protocols/5af1794d6a65a50007755b6a',
                    'title': 'A single protocol'
                },
                'submissionEnvelope': {
                    'href': 'http://api.ingest.dev.data.humancellatlas.org/protocols/5af1794d6a65a50007755b6a'
                            '/submissionEnvelope',
                    'title': 'Access or create new submission envelopes'
                }
            }
        }

        ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = MagicMock(name='mock_ingest_api')
        mock_ingest_api.load_root = MagicMock()
        mock_ingest_api.create_entity = MagicMock(return_value=new_entity_mock_response)

        submission = Submission(mock_ingest_api, submission_url='url')

        entity = Entity(entity_id='id', entity_type='biomaterial', content={})
        entity = submission.add_entity(entity)

        self.assertEqual(new_entity_mock_response, entity.ingest_json)

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


class IngestSubmitterTest(TestCase):

    def setUp(self) -> None:
        self.ingest_api = MagicMock('mock_ingest_api')
        self.ingest_api.get_submission = MagicMock()
        self.ingest_api.create_submission_manifest = MagicMock()
        self.ingest_api.patch = MagicMock()
        self.ingest_api.get_link_from_resource = MagicMock()

    @patch('ingest.importer.submission.ingest_submitter.Submission')
    def test_submit(self, submission_constructor):
        # given:
        submission = self._mock_submission(submission_constructor)

        # and:
        product = Entity('product', 'product_1', {})
        project = Entity('project', 'id', {})
        user = Entity('user', 'user_1', {})
        entity_map = EntityMap(product, user, project)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.add_entities(entity_map, submission_url='url')

        # then:
        submission_constructor.assert_called_with(self.ingest_api, 'url')
        submission.define_manifest.assert_called_with(entity_map)
        submission.add_entity.assert_has_calls([call(product), call(user)], any_order=True)

    def test_update_entities(self):
        # given:
        entity_map = self._create_test_entity_map()
        user2 = entity_map.get_entity('user', 'user_2')
        user3 = entity_map.get_entity('user', 'user_3')

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity = MagicMock()
        submitter.update_entities(entity_map)

        # then:
        submitter.update_entity.assert_has_calls([call(user2), call(user3)], any_order=True)

    def _create_test_entity_map(self) -> EntityMap:
        product = Entity('product', 'product_1', {'k': 'v'})
        project = Entity('project', 'id', {'k': 'v'})
        user1 = Entity('user', 'user_1', {'k': 'v'})
        user2 = Entity('user', 'user_2', {'k': 'v'}, {'content': {'k': 'v0'}, '_links': {'self': {'href': 'url'}}},
                       is_reference=True)
        user3 = Entity('user', 'user_3', {'k': 'v'}, {'content': {'k': 'v0'}, '_links': {'self': {'href': 'url'}}},
                       is_reference=True)
        entity_map = EntityMap(product, user1, user2, user3, project)
        return entity_map

    @patch('ingest.importer.submission.ingest_submitter.Submission')
    def test_submit_linked_entity(self, submission_constructor):
        # given:
        submission = self._mock_submission(submission_constructor)

        # and:
        user = Entity('user', 'user_1', {})
        entity_map = EntityMap(user)

        # and:
        link_to_user = {
            'entity': 'user',
            'id': 'user_1',
            'relationship': 'wish_list'
        }
        linked_product = Entity('product', 'product_1', {}, direct_links=[link_to_user], is_reference=False,
                                is_linking_reference=False)
        project = Entity('project', 'id', {}, is_reference=False, is_linking_reference=False)
        entity_map.add_entity(linked_product)
        entity_map.add_entity(project)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.link_submission_to_project = MagicMock()
        submitter.PROGRESS_CTR = 1
        submitter.add_entities(entity_map, submission_url='url')

        # then:
        submission_constructor.assert_called_with(self.ingest_api, 'url')
        submission.define_manifest.assert_called_with(entity_map)
        submission.add_entity.assert_has_calls([call(user), call(linked_product)], any_order=True)
        submission.link_entity.assert_called_with(linked_product, user, relationship='wish_list', is_collection=True)
        self.ingest_api.patch.assert_called_once()

    def test_update_entity(self):
        # given:
        user1 = Entity('user', 'user_1', {'k': 'v2'}, {'content': {'k': 'v', 'k2': 'v2'}, '_links': {'self': {'href': 'url'}}})

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity(user1)

        # then:
        self.ingest_api.patch.assert_called_with('url', {'content': {'k': 'v2', 'k2': 'v2'}})

    def test_update_entity__given_empty_ingest_json__then_fetch_resource(self):
        # given:
        ingest_json = {'content': {'k': 'v', 'k2': 'v2'}, '_links': {'self': {'href': 'url'}}}
        self.ingest_api.get_entity_by_uuid = Mock(return_value=ingest_json)

        # and:
        user1 = Entity('biomaterial', 'biomaterial_uuid', {'k': 'v2'}, None)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity(user1)

        # then:
        self.ingest_api.patch.assert_called_with('url', {'content': {'k': 'v2', 'k2': 'v2'}})

    def test_update_entity__given_content_has_no_update__then_do_not_patch(self):
        # given:
        ingest_json = {'content': {'k': 'v2', 'k2': 'v2'}, '_links': {'self': {'href': 'url'}}}
        self.ingest_api.get_entity_by_uuid = Mock(return_value=ingest_json)

        # and:
        user1 = Entity('biomaterial', 'biomaterial_uuid', {'k': 'v2'}, None)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity(user1)

        # then:
        self.ingest_api.patch.assert_not_called()

    @staticmethod
    def _mock_submission(submission_constructor):
        submission = MagicMock('submission')
        submission.define_manifest = MagicMock()
        submission.add_entity = MagicMock()
        submission.update_entity = MagicMock()
        submission.link_entity = MagicMock()
        submission.manifest = {}
        submission.is_update = lambda: False
        submission_constructor.return_value = submission
        return submission


class EntityMapTest(TestCase):

    def test_load(self):
        # given:
        spreadsheet_json = _create_spreadsheet_json()

        # when:
        entity_map = EntityMap.load(spreadsheet_json)

        # then:
        self.assertEqual(['project', 'biomaterial', 'file', 'protocol'],
                         list(entity_map.get_entity_types()))

        # and:
        # TODO shouldn't entity id's be unique and that there's no need to specify entity type?
        biomaterial1 = entity_map.get_entity('biomaterial', 'biomaterial_id_1')
        self._assert_correct_entity(biomaterial1, entity_id='biomaterial_id_1',
                                    entity_type='biomaterial', content={'key': 'biomaterial_1'})

        # and:
        biomaterial2 = entity_map.get_entity('biomaterial', 'biomaterial_id_2')
        links = {'biomaterial': ['biomaterial_id_1'], 'process': ['process_id_1']}
        self._assert_correct_entity(biomaterial2, entity_id='biomaterial_id_2',
                                    entity_type='biomaterial', content={'key': 'biomaterial_2'},
                                    links=links)

        # and:
        protocol1 = entity_map.get_entity('protocol', 'protocol_id_1')
        self.assertEqual({'key': 'protocol_1'}, protocol1.content)

        # and:
        biomaterial3 = entity_map.get_entity('biomaterial', 'biomaterial_uuid')
        self.assertTrue(biomaterial3.is_linking_reference)

    def test_load__is_linking_reference(self):
        # given:
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_id': {
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
            }
        }

        # when:
        entity_map = EntityMap.load(spreadsheet_json)

        # then:
        self.assertEqual(['biomaterial'], list(entity_map.get_entity_types()))

        # and:
        biomaterial = entity_map.get_entity('biomaterial', 'biomaterial_uuid')
        self.assertTrue(biomaterial.is_linking_reference)

    def test_load__is_reference(self):
        # given:
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_uuid': {
                    'content': {
                        'key': 'value'
                    },
                    'is_reference': True
                }
            }
        }

        # when:
        entity_map = EntityMap.load(spreadsheet_json)

        # then:
        self.assertEqual(['biomaterial'], list(entity_map.get_entity_types()))

        # and:
        biomaterial = entity_map.get_entity('biomaterial', 'biomaterial_uuid')
        self.assertTrue(biomaterial.is_reference)

    def _assert_correct_entity(self, entity, entity_id='', content={}, entity_type='', links={}):
        self.assertTrue(entity)
        self.assertEqual(entity_id, entity.id)
        self.assertEqual(content, entity.content)
        self.assertEqual(entity_type, entity.type)
        self.assertEqual(links, entity.links_by_entity)

    def test_count_total(self):
        # given:
        zero_map = EntityMap()

        # and:
        one_map = EntityMap()
        one_map.add_entity(Entity('product', 'product_1', {}))

        # and:
        three_map = EntityMap()
        three_map.add_entity(Entity('profile', 'profile_1', {}))
        for product_id in range(0, 2):
            three_map.add_entity(Entity('product', f'product_{product_id}', {}))

        # expect:
        self.assertEqual(0, zero_map.count_total())
        self.assertEqual(1, one_map.count_total())
        self.assertEqual(3, three_map.count_total())

    def test_count_links(self):
        entity_map = EntityMap()

        # no element
        self.assertEqual(entity_map.count_links(), 0)

        # has 1 element without links
        entity_map.add_entity(Entity('product', 'product_0', {}))
        self.assertEqual(entity_map.count_links(), 0)

        # has 1 element with links
        entity_map.add_entity(Entity('product', 'product_1', {}, direct_links=[{}, {}, {}]))
        self.assertEqual(entity_map.count_links(), 3)

        # has many element with links
        entity_map.add_entity(Entity('product', 'product_2', {}, direct_links=[{}, {}, {}, {}]))
        self.assertEqual(entity_map.count_links(), 7)


class EntityLinkerTest(TestCase):

    def setUp(self):
        mocked_template_manager = MagicMock(name='template_manager')
        mocked_template_manager.get_schema_url = MagicMock(return_value='url')
        self.mocked_template_manager = mocked_template_manager

    def test_generate_direct_links_biomaterial_to_biomaterial_has_process(self):
        # given
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
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                },
                'biomaterial_id_2': {
                    'content': {
                        'key': 'biomaterial_2'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        }
                    ]
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)
        output = entity_linker.handle_links_from_spreadsheet()

        self._assert_equal_direct_links(expected_json, output)

    def _assert_equal_direct_links(self, expected_json, output):
        for entity_type, entities_dict in expected_json.items():
            for entity_id, entity_dict in entities_dict.items():
                expected_links = entities_dict[entity_id].get('direct_links')
                expected_links = expected_links if expected_links else []
                entity = output.get_entity(entity_type, entity_id)
                self.assertTrue(entity)

                for link in expected_links:
                    self.assertTrue(link in entity.direct_links, f'{json.dumps(link)} is not in direct links for '
                                                                 f'entity type {entity_type} and id {entity_id}')

    def test_generate_direct_links_biomaterial_to_biomaterial_no_process(self):
        # given
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
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                },
                'biomaterial_id_2': {
                    'content': {
                        'key': 'biomaterial_2'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        }
                    ]
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)
        output = entity_linker.handle_links_from_spreadsheet()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_file_no_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    }
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)
        output = entity_linker.handle_links_from_spreadsheet()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_file_has_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    }
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }

                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'file_id_2': {
                    'content': {
                        'key': 'file_2'
                    },
                    'links_by_entity': {
                        'file': ['file_id_1'],
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)
        output = entity_linker.handle_links_from_spreadsheet()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_file_to_biomaterial_has_process(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'process': ['process_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }
                }
            },
            'biomaterial': {
                'biomaterial_id_1': {
                    'content': {
                        'key': 'biomaterial_1'
                    }
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    }
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    }
                }
            }
        }

        expected_json = {
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
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'inputToProcesses'
                        }
                    ]
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'process',
                            'id': 'process_id_1',
                            'relationship': 'derivedByProcesses'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'projects'
                        },
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_1',
                            'relationship': 'protocols'
                        },
                        {
                            'entity': 'protocol',
                            'id': 'protocol_id_2',
                            'relationship': 'protocols'
                        }
                    ]
                }
            },
            'protocol': {
                'protocol_id_1': {
                    'content': {
                        'key': 'protocol_1'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                },
                'protocol_id_2': {
                    'content': {
                        'key': 'protocol_2'
                    },
                    'direct_links': [
                        {
                            'entity': 'project',
                            'id': 'dummy-project-id',
                            'relationship': 'project',
                            'is_collection': False
                        }
                    ]
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)
        output = entity_linker.handle_links_from_spreadsheet()

        self._assert_equal_direct_links(expected_json, output)

    def test_generate_direct_links_link_not_found_error(self):
        # given
        spreadsheet_json = {
            'project': {
                'dummy-project-id': {
                    'content': {
                        'key': 'project_1'
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_1'],
                        'protocol': ['protocol_id_1', 'protocol_id_2']
                    }
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        with self.assertRaises(LinkedEntityNotFound) as context:
            entity_linker.handle_links_from_spreadsheet()

        self.assertEqual('biomaterial', context.exception.entity)
        self.assertEqual('biomaterial_id_1', context.exception.id)

    def test_generate_direct_links_invalid_spreadsheet_link(self):
        # given
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
                    },
                    'links_by_entity': {
                        'file': ['file_id_1']
                    }
                }
            },
            'file': {
                'file_id_1': {
                    'content': {
                        'key': 'file_1'
                    }
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        with self.assertRaises(InvalidLinkInSpreadsheet) as context:
            entity_linker.handle_links_from_spreadsheet()

        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual('file', context.exception.link_entity_type)

        self.assertEqual('biomaterial_id_1', context.exception.from_entity.id)
        self.assertEqual('file_id_1', context.exception.link_entity_id)

    def test_generate_direct_links_multiple_process_links(self):
        # given
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
                    },
                    'links_by_entity': {
                        'process': ['process_id_1', 'process_id_2']
                    }
                }
            },
            'process': {
                'process_id_1': {
                    'content': {
                        'key': 'process_1'
                    }
                },
                'process_id_2': {
                    'content': {
                        'key': 'process_2'
                    }
                }
            }
        }

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        with self.assertRaises(MultipleProcessesFound) as context:
            entity_linker.handle_links_from_spreadsheet()

        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual(['process_id_1', 'process_id_2'], context.exception.process_ids)
