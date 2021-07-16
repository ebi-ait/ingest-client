import json
from unittest import TestCase

from ingest.downloader.flattener import Flattener


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
            }
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

        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_modules(self):
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
            'Project - Contributors': [
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
            }
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
                'headers': ['project.uuid', 'project.project_core.project_short_name',
                            'project.project_core.project_title', 'project.project_core.project_description'],
                'values': [
                    {
                        'project.uuid': 'uuid1',
                        'project.project_core.project_short_name': 'label',
                        'project.project_core.project_title': 'title',
                        'project.project_core.project_description': 'desc'
                    }
                ]},
            'Project - Contributors': {
                'headers': ['project.contributors.corresponding_contributor',
                            'project.contributors.country',
                            'project.contributors.email',
                            'project.contributors.institution',
                            'project.contributors.laboratory',
                            'project.contributors.name',
                            'project.contributors.project_role.ontology',
                            'project.contributors.project_role.ontology_label',
                            'project.contributors.project_role.text'
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
                'headers': ['donor_organism.uuid', 'donor_organism.biomaterial_core.biomaterial_id',
                            'donor_organism.biomaterial_core.biomaterial_description'],
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
        }

        # then
        self.assertEqual(actual, expected)
