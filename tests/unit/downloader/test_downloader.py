import json
from unittest import TestCase

from ingest.downloader.downloader import XlsDownloader


class XlsDownloaderTest(TestCase):

    def test_convert_json(self):
        self.maxDiff = None
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

        with open('project-list.json') as file:
            entity_list = json.load(file)

        # when
        downloader = XlsDownloader()
        actual = downloader.convert_json(entity_list)

        expected = {
            'project': [
                {
                    'project.uuid': 'uuid1',
                    'project.project_core.project_short_name': 'label',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc'
                }
            ]
        }

        # then
        self.assertEqual(actual, expected)
