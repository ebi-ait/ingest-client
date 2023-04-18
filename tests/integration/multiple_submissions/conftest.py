import os
from pathlib import Path

import pytest

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.importer.importer import XlsImporter
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter


@pytest.fixture
def ingest_api():
    api = IngestApi()
    # TODO could also use a token generator here using the GCP service account,
    #  see ingest-integration-tests repo for reference
    token = os.getenv('INGEST_TOKEN')
    if not token:
        raise Exception("INGEST_TOKEN env variable must be set")
    api.set_token(f"Bearer {token}")
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
