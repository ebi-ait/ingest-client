import os
import unittest
import requests

from unittest import TestCase
from tests.utils import delete_file

from ingest.api.ingestapi import IngestApi
from ingest.importer.importer import XlsImporter
from ingest.utils.s2s_token_client import S2STokenClient, ServiceCredential
from ingest.utils.token_manager import TokenManager

INGEST_API = os.environ.get('INGEST_API', 'https://api.ingest.dev.archive.data.humancellatlas.org/')
DEPLOYMENT = 'develop'
SPREADSHEET_FILE = 'dcp_integration_test_metadata_1_SS2_bundle.xlsx'
SPREADSHEET_LOCATION = f'https://raw.github.com/HumanCellAtlas/metadata-schema/{DEPLOYMENT}/infrastructure_testing_files' \
                       f'/current/{SPREADSHEET_FILE}'
INGEST_API_JWT_AUDIENCE = os.environ.get('INGEST_API_JWT_AUDIENCE', 'https://dev.data.humancellatlas.org/')


def download_file(url, path):
    response = requests.get(url)
    response.raise_for_status()
    with open(path, 'wb') as f:
        f.write(response.content)


@unittest.skipIf(not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
                 'The environment variable GOOGLE_APPLICATION_CREDENTIALS should contain the location of GCP credentials file')
class SpreadsheetImport(TestCase):
    def setUp(self):
        self.test_data_path = os.path.dirname(os.path.realpath(__file__))
        self.configure_ingest_client()

    def configure_ingest_client(self):
        gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.s2s_token_client = S2STokenClient(ServiceCredential.from_file(gcp_credentials_file),
                                               INGEST_API_JWT_AUDIENCE)
        self.token_manager = TokenManager(self.s2s_token_client)
        self.ingest_api = IngestApi(url=INGEST_API, token_manager=self.token_manager)

    def test_spreadsheet_import(self):
        self.metadata_spreadsheet_path = os.path.join(self.test_data_path, SPREADSHEET_FILE)
        download_file(SPREADSHEET_LOCATION, self.metadata_spreadsheet_path)
        importer = XlsImporter(self.ingest_api)
        submission_resource = self.ingest_api.create_submission()

        submission_url = submission_resource["_links"]["self"]["href"].rsplit("{")[0]
        submission, _ = importer.import_file(self.metadata_spreadsheet_path, submission_url, False)

        entities_by_type = {}

        for entity in submission.get_entities():
            entity_type = entity.type
            if not entities_by_type.get(entity_type):
                entities_by_type[entity_type] = []
            entities_by_type[entity_type].append(entity)

        files = list(self.ingest_api.get_entities(submission_url, 'files'))
        biomaterials = list(self.ingest_api.get_entities(submission_url, 'biomaterials'))
        protocols = list(self.ingest_api.get_entities(submission_url, 'protocols'))
        processes = list(self.ingest_api.get_entities(submission_url, 'processes'))

        self.assertEquals(len(files), len(entities_by_type['file']))
        self.assertEquals(len(biomaterials), len(entities_by_type['biomaterial']))
        self.assertEquals(len(protocols), len(entities_by_type['protocol']))
        self.assertEquals(len(processes), len(entities_by_type['process']))

    def tearDown(self) -> None:
        if self.metadata_spreadsheet_path:
            delete_file(self.metadata_spreadsheet_path)
