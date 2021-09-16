import json
from unittest import TestCase

from ingest.downloader.flattener import Flattener, Error as FlattenerError


class FlattenerTest(TestCase):
    def setUp(self) -> None:
        self.content = {
            "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
            "schema_type": "project",
            "project_core": {
                "project_short_name": "label",
                "project_title": "title",
                "project_description": "desc"
            }
        }

        self.uuid = {
            'uuid': 'uuid1'
        }

        self.flattened_metadata_entity = {
            'Project': {
                'headers': ['project.uuid', 'project.project_core.project_short_name',
                            'project.project_core.project_title', 'project.project_core.project_description'],
                'values': [
                    {
                        'project.uuid': 'uuid1',
                        'project.project_core.project_short_name': 'label',
                        'project.project_core.project_title': 'title',
                        'project.project_core.project_description': 'desc'
                    }
                ]
            },
            'Schemas': [
                'https://schema.humancellatlas.org/type/project/14.2.0/project'
            ]
        }

    def test_flatten__has_no_modules(self):
        # given
        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }
        entity_list = [self.metadata_entity]

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
        self.content.update({"contributors": [
            {
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
            }
        ]})
        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

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
                'values': [
                    {'project.contributors.corresponding_contributor': 'True',
                     'project.contributors.country': 'USA',
                     'project.contributors.email': 'alex.pollen@ucsf.edu',
                     'project.contributors.institution': 'University of California, San Francisco (UCSF)',
                     'project.contributors.laboratory': 'Department of Neurology',
                     'project.contributors.name': 'Alex A,,Pollen',
                     'project.contributors.project_role.ontology': 'EFO:0009741',
                     'project.contributors.project_role.ontology_label': 'experimental scientist',
                     'project.contributors.project_role.text': 'experimental scientist'}
                ]
            }
        })

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_single_element(self):
        # given
        self.content.update(
            {"organ_parts": [
                {
                    "ontology": "UBERON:0000376",
                    "ontology_label": "hindlimb stylopod",
                    "text": "hindlimb stylopod"
                }
            ]})

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.organ_parts.ontology': 'UBERON:0000376',
            'project.organ_parts.ontology_label': 'hindlimb stylopod',
            'project.organ_parts.text': 'hindlimb stylopod',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.organ_parts.ontology',
                'project.organ_parts.ontology_label',
                'project.organ_parts.text'
            ]
        )

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

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)
        flattened_metadata_entity = {
            'Collection Protocol': {
                'values': [{

                }],
                'headers': []
            }
        }
        flattened_metadata_entity['Collection Protocol']['values'][0].update({
            'collection_protocol.organ_parts.field_1': 'UBERON:0000376',
            'collection_protocol.organ_parts.field_2': 'hindlimb stylopod',
            'collection_protocol.organ_parts.field_3': 'hindlimb stylopod',

        })
        flattened_metadata_entity['Collection Protocol']['headers'].extend(
            [
                'collection_protocol.organ_parts.field_1',
                'collection_protocol.organ_parts.field_2',
                'collection_protocol.organ_parts.field_3'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_multiple_elements(self):
        # given
        self.content.update(
            {'organ_parts': [
                {
                    'ontology': 'UBERON:0000376',
                    'ontology_label': 'dummylabel1',
                    'text': 'dummytext1'
                },
                {
                    'ontology': 'UBERON:0002386',
                    'ontology_label': 'dummylabel2',
                    'text': 'dummytext2'
                }
            ]})

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.organ_parts.ontology': 'UBERON:0000376||UBERON:0002386',
            'project.organ_parts.ontology_label': 'dummylabel1||dummylabel2',
            'project.organ_parts.text': 'dummytext1||dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.organ_parts.ontology',
                'project.organ_parts.ontology_label',
                'project.organ_parts.text'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_multiple_elements_but_inconsistent_columns(self):
        # given
        self.content.update(
            {'diseases': [
                {
                    'ontology': 'UBERON:0000376',
                    'ontology_label': 'dummylabel1',
                    'text': 'dummytext1'
                },
                {
                    'text': 'dummytext2'
                }
            ]})

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.diseases.ontology': 'UBERON:0000376',
            'project.diseases.ontology_label': 'dummylabel1',
            'project.diseases.text': 'dummytext1||dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.diseases.ontology',
                'project.diseases.ontology_label',
                'project.diseases.text'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_multiple_elements_but_with_empty_ontology_values(self):
        # given
        self.content.update(
            {'diseases': [
                {
                    'ontology': 'UBERON:0000376',
                    'ontology_label': 'dummylabel1',
                    'text': 'dummytext1'
                },
                {
                    'ontology': '',
                    'ontology_label': '',
                    'text': 'dummytext2'
                }
            ]})

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.diseases.ontology': 'UBERON:0000376',
            'project.diseases.ontology_label': 'dummylabel1',
            'project.diseases.text': 'dummytext1||dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.diseases.ontology',
                'project.diseases.ontology_label',
                'project.diseases.text'
            ]
        )

        # then
        self.assertEqual(self.flattened_metadata_entity, actual)

    def test_flatten__has_ontology_property_with_single_element_but_only_with_text_attr(self):
        # given
        self.content.update(
            {'diseases': [
                {
                    'text': 'dummytext2'
                }
            ]})

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.diseases.text': 'dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.diseases.text'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

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

    def test_flatten__project_metadata(self):
        self.maxDiff = None

        # given
        with open('project-list.json') as file:
            entity_list = json.load(file)

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        with open('project-list-flattened.json') as file:
            expected = json.load(file)

        # then
        self.assertEqual(actual, expected)

    def test_flatten__has_different_entities(self):
        # given
        entity_list = [{
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
                "schema_type": "project",
                "project_core": {
                    "project_short_name": "label",
                    "project_title": "title",
                    "project_description": "desc"
                },
                "contributors": [
                    {
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
                    }
                ]
            },
            'uuid': {
                'uuid': 'uuid1'
            }
        },
            {
                'content': {
                    "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/donor_organism",
                    "schema_type": "biomaterial",
                    "biomaterial_core": {
                        "biomaterial_id": "label",
                        "biomaterial_description": "desc"
                    }
                },
                'uuid': {
                    'uuid': 'uuid2'
                }
            },
            {
                'content': {
                    "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/donor_organism",
                    "schema_type": "biomaterial",
                    "biomaterial_core": {
                        "biomaterial_id": "label",
                        "biomaterial_description": "desc"
                    }
                },
                'uuid': {
                    'uuid': 'uuid3'
                }
            }
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
                        'project.project_core.project_short_name': 'label',
                        'project.project_core.project_title': 'title',
                        'project.project_core.project_description': 'desc'
                    }
                ]},
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
                'values': [
                    {'project.contributors.corresponding_contributor': 'True',
                     'project.contributors.country': 'USA',
                     'project.contributors.email': 'alex.pollen@ucsf.edu',
                     'project.contributors.institution': 'University of California, San Francisco (UCSF)',
                     'project.contributors.laboratory': 'Department of Neurology',
                     'project.contributors.name': 'Alex A,,Pollen',
                     'project.contributors.project_role.ontology': 'EFO:0009741',
                     'project.contributors.project_role.ontology_label': 'experimental scientist',
                     'project.contributors.project_role.text': 'experimental scientist'}
                ]
            },
            'Donor organism': {
                'headers': [
                    'donor_organism.uuid',
                    'donor_organism.biomaterial_core.biomaterial_id',
                    'donor_organism.biomaterial_core.biomaterial_description'
                ],
                'values': [
                    {
                        'donor_organism.uuid': 'uuid2',
                        'donor_organism.biomaterial_core.biomaterial_id': 'label',
                        'donor_organism.biomaterial_core.biomaterial_description': 'desc'
                    },
                    {
                        'donor_organism.uuid': 'uuid3',
                        'donor_organism.biomaterial_core.biomaterial_id': 'label',
                        'donor_organism.biomaterial_core.biomaterial_description': 'desc'
                    }
                ]
            },
            'Schemas': [
                'https://schema.humancellatlas.org/type/project/14.2.0/project',
                'https://schema.humancellatlas.org/type/project/14.2.0/donor_organism'
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

    def test_flatten__collection_protocol_metadata(self):
        # given
        with open('collection_protocol-list.json') as file:
            entity_list = json.load(file)

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        with open('collection_protocol-list-flattened.json') as file:
            expected = json.load(file)

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
        with self.assertRaisesRegex(FlattenerError, "Multiple versions of same concrete entity schema"):
            flattener.flatten(entity_list)

