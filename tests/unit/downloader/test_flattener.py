import json
import os
from unittest import TestCase

from ingest.downloader.entity import Entity
from ingest.downloader.flattener import Flattener


class FlattenerTest(TestCase):
    def setUp(self) -> None:
        self.script_dir = os.path.dirname(__file__)

        with open(self.script_dir + '/content.json') as file:
            self.content = json.load(file)

        self.uuid = {
            'uuid': 'uuid1'
        }

        with open(self.script_dir + '/content-flattened.json') as file:
            self.flattened_metadata_entity = json.load(file)

    def test_flatten__has_no_modules(self):
        # given
        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }
        entity_list = [Entity.from_json(metadata_entity)]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_string_arrays(self):
        # given
        self.content.update({
            "insdc_project_accessions": [
                "SRP180337"
            ],
            "geo_series_accessions": [
                "GSE124298", "GSE124299"
            ]
        })
        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }
        entity_list = [Entity.from_json(self.metadata_entity)]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.insdc_project_accessions': 'SRP180337',
            'project.geo_series_accessions': "GSE124298||GSE124299"
        })

        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.insdc_project_accessions',
                'project.geo_series_accessions'
            ]
        )

        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_list_property_with_elements(self):
        # given
        content = {
            "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/collection_protocol",
            "schema_type": "protocol",
            "organ_parts": [
                {
                    "field_1": "UBERON:0000376",
                    "field_2": "hindlimb stylopod",
                    "field_3": "hindlimb stylopod"
                }
            ]
        }

        metadata_entity = {
            'content': content,
            'uuid': self.uuid
        }

        entity_list = [Entity.from_json(metadata_entity)]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        flattened_metadata_entity = {
            'Collection protocol': {
                'values': [{
                    'collection_protocol.organ_parts.field_1': 'UBERON:0000376',
                    'collection_protocol.organ_parts.field_2': 'hindlimb stylopod',
                    'collection_protocol.organ_parts.field_3': 'hindlimb stylopod',
                    'collection_protocol.uuid': 'uuid1'
                }],
                'headers': [
                    'collection_protocol.uuid',
                    'collection_protocol.organ_parts.field_1',
                    'collection_protocol.organ_parts.field_2',
                    'collection_protocol.organ_parts.field_3'
                ]
            },
            'Schemas': [
                'https://schema.humancellatlas.org/type/project/14.2.0/collection_protocol'
            ]
        }

        # then
        self.assertEqual(actual, flattened_metadata_entity)

    def test_flatten__has_boolean(self):
        # given
        self.content.update({
            'boolean_field': True
        })
        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }
        entity_list = [Entity.from_json(self.metadata_entity)]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.boolean_field': 'True'
        })
        self.flattened_metadata_entity['Project']['headers'].append('project.boolean_field')

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_integer(self):
        # given
        self.content.update({
            'int_field': 1
        })
        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }
        entity_list = [Entity.from_json(self.metadata_entity)]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.int_field': '1'
        })

        self.flattened_metadata_entity['Project']['headers'].append('project.int_field')

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__rows_have_different_columns(self):
        # given
        entity_json_list = [
            {
                'content': {
                    "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
                    "schema_type": "project",
                    "project_core": {
                        "project_short_name": "label1",
                    }
                },
                'uuid': {
                    'uuid': 'uuid1'
                }
            },
            {
                'content': {
                    "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
                    "schema_type": "project",
                    "project_core": {
                        "project_short_name": "label2",
                        "project_title": "title",
                        "project_description": "desc"
                    }
                },
                'uuid': {
                    'uuid': 'uuid2'
                }
            },

        ]

        # when
        flattener = Flattener()
        entity_list = [Entity.from_json(entity_json) for entity_json in entity_json_list]
        actual = flattener.flatten(entity_list)

        expected = {
            'Project': {
                'headers': [
                    'project.uuid',
                    'project.project_core.project_short_name',
                    'project.project_core.project_title',
                    'project.project_core.project_description'
                ],
                'values': [
                    {
                        'project.uuid': 'uuid1',
                        'project.project_core.project_short_name': 'label1'
                    },
                    {
                        'project.uuid': 'uuid2',
                        'project.project_core.project_short_name': 'label2',
                        'project.project_core.project_title': 'title',
                        'project.project_core.project_description': 'desc'
                    }
                ]
            },
            'Schemas': [
                'https://schema.humancellatlas.org/type/project/14.2.0/project'
            ]
        }

        # then
        self.assertEqual(actual, expected)

    def test_flatten__raises_error__given_multiple_schema_versions_of_same_concrete_entity(self):
        # given
        entity_json_list = [
            {
                'content': {
                    "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/donor_organism",
                    "schema_type": "biomaterial",
                    "field": "value"
                },
                'uuid': {
                    'uuid': 'uuid1'
                }
            },
            {
                'content': {
                    "describedBy": "https://schema.humancellatlas.org/type/project/14.3.0/donor_organism",
                    "schema_type": "biomaterial",
                    "field": "value"
                },
                'uuid': {
                    'uuid': 'uuid2'
                }
            },

        ]
        entity_list = Entity.from_json_list(entity_json_list)

        # when/then
        flattener = Flattener()
        with self.assertRaisesRegex(ValueError, "Multiple versions of same concrete entity schema"):
            flattener.flatten(entity_list)

    def test_flatten__entities_has_input(self):
        # given
        metadata_entity = Entity.from_json({
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/biomaterial/14.2.0/specimen_from_organism",
                "schema_type": "biomaterial",
                "field": "value",
                'biomaterial_core': {
                    'biomaterial_id': 'specimen_id'
                }
            },
            'uuid': {
                'uuid': 'specimen-uuid'
            }
        })

        input_entity = Entity.from_json({
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/biomaterial/14.2.0/donor_organism",
                "schema_type": "biomaterial",
                "field": "value",
                'biomaterial_core': {
                    'biomaterial_id': 'donor_id'
                }
            },
            'uuid': {
                'uuid': 'donor-uuid'
            }
        })

        process = Entity.from_json({
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/process/14.2.0/process",
                "schema_type": "process",
                "field": "value",
                'process_core': {
                    'process_id': 'process_id'
                }
            },
            'uuid': {
                'uuid': 'process-uuid'
            }
        })

        protocols = Entity.from_json_list([{
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/protocol/14.2.0/sequencing_protocol",
                "schema_type": "protocol",
                "field": "value",
                'protocol_core': {
                    'protocol_id': 'protocol_id1'
                }
            },
            'uuid': {
                'uuid': 'protocol-uuid1'
            }

        }, {
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/protocol/14.2.0/library_preparation_protocol",
                "schema_type": "protocol",
                "field": "value",
                'protocol_core': {
                    'protocol_id': 'protocol_id2'
                }
            },
            'uuid': {
                'uuid': 'protocol-uuid2'
            }
        }])

        metadata_entity.set_input([input_entity], process, protocols)
        # when
        flattener = Flattener()
        actual = flattener.flatten([metadata_entity])

        expected = {
            'Schemas': ['https://schema.humancellatlas.org/type/biomaterial/14.2.0/specimen_from_organism'],
            'Specimen from organism': {'headers': ['specimen_from_organism.uuid',
                                                   'specimen_from_organism.field',
                                                   'specimen_from_organism.biomaterial_core.biomaterial_id',
                                                   'process.field',
                                                   'process.process_core.process_id',
                                                   'sequencing_protocol.protocol_core.protocol_id',
                                                   'sequencing_protocol.uuid',
                                                   'library_preparation_protocol.protocol_core.protocol_id',
                                                   'library_preparation_protocol.uuid',
                                                   'donor_organism.biomaterial_core.biomaterial_id',
                                                   'donor_organism.uuid'],
                                       'values': [
                                           {'specimen_from_organism.biomaterial_core.biomaterial_id': 'specimen_id',
                                            'donor_organism.biomaterial_core.biomaterial_id': 'donor_id',
                                            'donor_organism.uuid': 'donor-uuid',
                                            'specimen_from_organism.field': 'value',
                                            'library_preparation_protocol.protocol_core.protocol_id': 'protocol_id2',
                                            'library_preparation_protocol.uuid': 'protocol-uuid2',
                                            'process.field': 'value',
                                            'process.process_core.process_id': 'process_id',
                                            'sequencing_protocol.protocol_core.protocol_id': 'protocol_id1',
                                            'sequencing_protocol.uuid': 'protocol-uuid1',
                                            'specimen_from_organism.uuid': 'specimen-uuid'}]}}
        self.assertEqual(actual, expected)
