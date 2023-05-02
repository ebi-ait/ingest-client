import os
from pathlib import Path

import pytest

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.importer.importer import XlsImporter
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter
from hca_ingest.utils.s2s_token_client import ServiceCredential, S2STokenClient
from hca_ingest.utils.token_manager import TokenManager


@pytest.fixture
def ingest_api():
    gcp_credentials_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    credential = ServiceCredential.from_file(gcp_credentials_file)
    audience = os.environ.get('INGEST_API_JWT_AUDIENCE')
    s2s_token_client = S2STokenClient(credential, audience)
    token_manager = TokenManager(s2s_token_client)
    api = IngestApi(token_manager=token_manager)
    return api


@pytest.fixture()
def downloader(ingest_api):
    return WorkbookDownloader(ingest_api)


@pytest.fixture()
def importer(ingest_api):
    return XlsImporter(ingest_api)


@pytest.fixture()
def submitter(ingest_api) -> IngestSubmitter:
    return IngestSubmitter(ingest_api)


@pytest.fixture()
def data_path():
    return Path(__file__).parent / 'data'


@pytest.fixture()
def output_path():
    output_path = Path('.') / 'output'
    output_path.mkdir(exist_ok=True)
    return output_path
