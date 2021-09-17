import json
import os
from unittest import TestCase

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
        entity_list = [metadata_entity]

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
        entity_list = [self.metadata_entity]

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

    def test_flatten__has_project_modules(self):
        # given
        self.content.update({
            "contributors": [{
                "name": "Alex A,,Pollen",
                "email": "alex.pollen@ucsf.edu",
                "institution": "University of California, San Francisco (UCSF)",
                "laboratory": "Department of Neurology",
                "country": "USA",
                "corresponding_contributor": True,
                "project_role": {
                    "text": "experimental scientist",
                    "ontology": "EFO:0009741",
                    "ontology_label": "experimental scientist"
                }
            }]
        })

        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity.update({
            'Project - Contributors': {
                'headers': [
                    'project.contributors.name',
                    'project.contributors.email',
                    'project.contributors.institution',
                    'project.contributors.laboratory',
                    'project.contributors.country',
                    'project.contributors.corresponding_contributor',
                    'project.contributors.project_role.text',
                    'project.contributors.project_role.ontology',
                    'project.contributors.project_role.ontology_label'
                ],
                'values': [{
                    'project.contributors.corresponding_contributor': 'True',
                    'project.contributors.country': 'USA',
                    'project.contributors.email': 'alex.pollen@ucsf.edu',
                    'project.contributors.institution': 'University of California, San Francisco (UCSF)',
                    'project.contributors.laboratory': 'Department of Neurology',
                    'project.contributors.name': 'Alex A,,Pollen',
                    'project.contributors.project_role.ontology': 'EFO:0009741',
                    'project.contributors.project_role.ontology_label': 'experimental scientist',
                    'project.contributors.project_role.text': 'experimental scientist'}
                ]}
        })

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_property_with_elements(self):
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

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)
        flattened_metadata_entity = {
            'Collection protocol': {
                'values': [{}],
                'headers': []
            }
        }
        flattened_metadata_entity['Collection protocol']['values'][0].update({
            'collection_protocol.organ_parts.field_1': 'UBERON:0000376',
            'collection_protocol.organ_parts.field_2': 'hindlimb stylopod',
            'collection_protocol.organ_parts.field_3': 'hindlimb stylopod',
            'collection_protocol.uuid': 'uuid1'
        })
        flattened_metadata_entity['Collection protocol']['headers'].extend(
            [
                'collection_protocol.uuid',
                'collection_protocol.organ_parts.field_1',
                'collection_protocol.organ_parts.field_2',
                'collection_protocol.organ_parts.field_3'
            ]
        )
        flattened_metadata_entity['Schemas'] = [
            'https://schema.humancellatlas.org/type/project/14.2.0/collection_protocol'
        ]

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
        entity_list = [self.metadata_entity]

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
        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        expected = {
            'Project': {
                'headers': [
                    'project.uuid',
                    'project.project_core.project_short_name',
                    'project.project_core.project_title',
                    'project.project_core.project_description',
                    'project.int_field'
                ],
                'values': [
                    {
                        'project.uuid': 'uuid1',
                        'project.project_core.project_short_name': 'label',
                        'project.project_core.project_title': 'title',
                        'project.project_core.project_description': 'desc',
                        'project.int_field': '1'
                    }
                ]
            },
            'Schemas': [
                'https://schema.humancellatlas.org/type/project/14.2.0/project'
            ]
        }

        # then
        self.assertEqual(actual, expected)

    def test_flatten__rows_have_different_columns(self):
        # given
        entity_list = [
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
        entity_list = [
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

        # when/then
        flattener = Flattener()
        with self.assertRaisesRegex(ValueError, "Multiple versions of same concrete entity schema"):
            flattener.flatten(entity_list)
