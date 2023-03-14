# test spreadsheet export for project with more than a single submission.
import dataclasses
import os
from pathlib import Path
from typing import List

import pytest

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.importer.importer import XlsImporter
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter


@dataclasses.dataclass
class TestContext:
    submissions: List[str]
    project_uuid:str

# given a project
# with spreadsheet uploaded
# and exported
# when a new submission is uploaded
# and exported
# then generated spreadsheet contains everything

@pytest.fixture
def ingest_api():
    api = IngestApi()
    api.token = os.getenv('INGEST_TOKEN')
    return api


@pytest.fixture()
def downloader(ingest_api):
    return WorkbookDownloader(ingest_api)


@pytest.fixture()
def importer(ingest_api):
    return XlsImporter(ingest_api)


@pytest.fixture()
def data_path():
    return Path(__file__).parent / 'data'

@pytest.fixture()
def submitter(ingest_api):
    return IngestSubmitter(ingest_api)

def test_generate_spreadsheet(ingest_api, downloader, importer, data_path, submitter):
    # upload submission 1
    filename = 'submission1.xlsx'
    submission = ingest_api.create_submission()
    import_spreadsheet(filename,
                       submission_url=submission['_links']['self']['href'],
                       data_path=data_path,
                       importer=importer)
    export_to_spreadsheet(filename, submission["uuid"]["uuid"], downloader)
    #
    project_uuid = get_project_uuid(ingest_api, submission)
    exported_url = ingest_api.get_link_from_resource(submission, 'commitExported')
    ingest_api.put(exported_url)

    submission2 = ingest_api.create_submission()
    submission2_url=submission2['_links']['self']['href']
    submitter.link_submission_to_project(project_uuid, submission2_url)
    add_file(ingest_api,
             submission_url=submission2_url,
             filename='new_file.txt',
             file_type='sequence_file')
    export_to_spreadsheet(filename='submission2.xlsx',
                          submission_uuid=submission2["uuid"]["uuid"],
                          downloader=downloader)


def add_file(ingest_api:IngestApi, submission_url, filename, file_type):
    schema_url = ingest_api.get_latest_schema_url('type', 'file', file_type)
    spreadsheet_payload = build_file_payload(schema_url, filename)
    file_entity = ingest_api.create_file(
        submission_url,
        filename=filename,
        content=spreadsheet_payload
    )
    return file_entity

def build_file_payload(schema_url, filename):
    return {
        "describedBy": schema_url,
        "schema_type": "file",
        "file_core": {
            "file_name": filename,
            "format": os.path.splitext(filename)[1][1:] or 'n/a',
        }
    }

def get_project_uuid(ingest_api, submission):
    projects_url = ingest_api.get_link_from_resource(submission, 'projects')
    project_uuid = ingest_api.get(projects_url).json().get('_embedded', {}).get('projects', [])[0]['uuid']['uuid']
    return project_uuid


def import_spreadsheet(filename: str,
                       submission_url,
                       data_path: Path,
                       importer: XlsImporter,
                       project_uuid=None,
                       is_update=False):
    path = data_path / filename
    imported_submission, template_manager = importer.import_file(path,
                                                                 submission_url,
                                                                 project_uuid=project_uuid,
                                                                 update_project=False,
                                                                 is_update=is_update)
    return imported_submission


def export_to_spreadsheet(filename, submission_uuid, downloader: WorkbookDownloader):
    workbook = downloader.get_workbook_from_submission(submission_uuid)
    output_path = Path('.') / 'output'
    output_path.mkdir(exist_ok=True)
    workbook.save(output_path / filename)