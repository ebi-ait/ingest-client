import json
from unittest import TestCase

from ingest.downloader.downloader import XlsDownloader


class XlsDownloaderTest(TestCase):
    def test_convert_json__has_no_modules(self):
        # given
        entity_list = [{
            'content': {
                "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
                "schema_type": "project",
                "project_core": {
                    "project_short_name": "label",
                    "project_title": "title",
                    "project_description": "desc"
                }
            },
            'uuid': {
                'uuid': 'uuid1'
            }
        }]

        # when
        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'Project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc'
                }
            ]
        }

        self.assertEqual(actual, expected)

    def test_convert_json__has_string_arrays(self):
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
                "insdc_project_accessions": [
                    "SRP180337"
                ],
                "geo_series_accessions": [
                    "GSE124298", "GSE124299"
                ]
            },
            'uuid': {
                'uuid': 'uuid1'
            }
        }]

        # when
        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'Project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc',
                    'project.insdc_project_accessions': 'SRP180337',
                    'project.geo_series_accessions': "GSE124298||GSE124299"
                }
            ]
        }

        self.assertEqual(actual, expected)

    def test_convert_json__has_modules(self):
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
        }]

        # when

        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'Project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc'
                }
            ],
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
        }

        # then
        self.assertEqual(actual, expected)

    def test_convert_json__has_boolean(self):
        # given
        entity_list = [{
            'content': {
                'describedBy': 'https://schema.humancellatlas.org/type/project/14.2.0/project',
                'schema_type': 'project',
                'project_core': {
                    'project_short_name': 'label',
                    'project_title': 'title',
                    'project_description': 'desc'
                },
                'boolean_field': True
            },
            'uuid': {
                'uuid': 'uuid1'
            }
        }]

        # when
        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'Project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc',
                    'project.boolean_field': 'True'
                }
            ]
        }

        # then
        self.assertEqual(actual, expected)

    def test_convert_json__has_integer(self):
        # given
        entity_list = [{
            'content': {
                'describedBy': 'https://schema.humancellatlas.org/type/project/14.2.0/project',
                'schema_type': 'project',
                'project_core': {
                    'project_short_name': 'label',
                    'project_title': 'title',
                    'project_description': 'desc'
                },
                'int_field': 1
            },
            'uuid': {
                'uuid': 'uuid1'
            }
        }]

        # when
        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'Project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc',
                    'project.int_field': '1'
                }
            ]
        }

        # then
        self.assertEqual(actual, expected)

    def test_convert_json__project_metadata(self):
        self.maxDiff = None

        # given
        with open('project-list.json') as file:
            entity_list = json.load(file)

        # when
        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        with open('project-list-flattened.json') as file:
            expected = json.load(file)

        # then
        self.assertEqual(actual, expected)

    def test_convert_json__has_different_entities(self):
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
                'uuid': 'uuid1'
            }
        }
        ]

        # when

        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'Project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc'
                }
            ],
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
            ],
            'Donor organism': [
                {
                    'donor_organism.uuid': 'uuid1',
                    'donor_organism.biomaterial_core.biomaterial_id': 'label',
                    'donor_organism.biomaterial_core.biomaterial_description': 'desc'
                }
            ],
        }

        # then
        self.assertEqual(actual, expected)
