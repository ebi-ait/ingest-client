import json
import os
from unittest import TestCase
from unittest.mock import MagicMock

from ingest.importer.submission.entity_linker import EntityLinker
from ingest.importer.submission.entity_map import EntityMap
from ingest.importer.submission.errors import MultipleProcessesFound, InvalidLinkInSpreadsheet, LinkedEntityNotFound


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

    def test_handle_links_from_spreadsheet__with_external_links(self):
        # given
        with open(os.path.dirname(__file__) + '/spreadsheet_with_external_links.json') as file:
            spreadsheet_json = json.load(file)
        mocked_template_manager = MagicMock(name='template_manager')
        mocked_template_manager.get_schema_url = MagicMock(return_value='url')
        self.mocked_template_manager = mocked_template_manager

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        # when
        output = entity_linker.handle_links_from_spreadsheet()

        # then
        lib_prep_protocol = output.get_entity('protocol', 'librep-protocol-uuid')
        self.assertTrue(lib_prep_protocol.is_linking_reference)
        self.assertTrue(lib_prep_protocol.is_reference)

        seq_protocol = output.get_entity('protocol', 'seq-protocol-uuid')
        self.assertTrue(seq_protocol.is_linking_reference)
        self.assertTrue(seq_protocol.is_reference)

        cell_suspension = output.get_entity('biomaterial', 'cell-suspension-uuid')
        self.assertTrue(cell_suspension.is_linking_reference)
        self.assertTrue(cell_suspension.is_reference)

        file1 = output.get_entity('file', 'seq-file-uuid-1')
        self.assertFalse(file1.is_linking_reference)
        self.assertTrue(file1.is_reference)

        file2 = output.get_entity('file', 'seq-file-uuid-2')
        self.assertFalse(file2.is_linking_reference)
        self.assertTrue(file2.is_reference)

        assay_process = output.get_entity('process', 'assay_process-uuid')
        self.assertTrue(assay_process.is_linking_reference)
        self.assertTrue(assay_process.is_reference)
        assay_process_content = {
            'process_core': {
                'process_description': 'desc', 'process_id': 'assay_process'
            },
            'schema_type': 'process',
            'describedBy': 'url'
        }
        self.assertEqual(assay_process.content, assay_process_content)
