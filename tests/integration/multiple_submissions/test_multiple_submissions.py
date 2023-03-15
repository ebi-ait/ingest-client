# test spreadsheet export for project with more than a single submission.
import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from openpyxl.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from pytest_bdd import given, parsers, when, then, scenario

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.importer.importer import XlsImporter
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter


@scenario('multiple_submissions.feature', 'Spreadsheet upload then add new file using api')
def test_multiple():
    pass


@dataclass
class TestContext:
    submission: dict = None
    project_uuid:str = ''
    new_submission: dict = None


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
def submitter(ingest_api) -> IngestSubmitter:
    return IngestSubmitter(ingest_api)


def _test_generate_spreadsheet(ingest_api, downloader, importer, data_path, submitter):
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
    submission2_url = submission2['_links']['self']['href']
    submitter.link_submission_to_project(project_uuid, submission2_url)
    add_file(ingest_api,
             submission_url=submission2_url,
             filename='new_file.txt',
             file_type='sequence_file')
    export_to_spreadsheet(filename='submission2.xlsx',
                          submission_uuid=submission2["uuid"]["uuid"],
                          downloader=downloader)


@given(parsers.parse('add a new file called {filename} with type {file_type}'))
def add_file(ingest_api:IngestApi, new_submission, filename, file_type):
    schema_url = ingest_api.get_latest_schema_url('type', 'file', file_type)
    spreadsheet_payload = build_file_payload(schema_url, filename)
    submission_url = new_submission['_links']['self']['href']
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


@given(parsers.parse('upload spreadsheet {filename}'))
def import_spreadsheet(filename: str,
                       submission,
                       data_path: Path,
                       importer: XlsImporter):
    path = data_path / filename
    submission_url = submission['_links']['self']['href']
    imported_submission, template_manager = importer.import_file(path, submission_url)
    return imported_submission


@when('I export a spreadsheet',
      target_fixture='spreadsheet')
def export_to_spreadsheet(new_submission, downloader: WorkbookDownloader):
    submission_uuid = new_submission["uuid"]["uuid"]
    filename = f'{submission_uuid}.xlsx'
    workbook = downloader.get_workbook_from_submission(submission_uuid)
    output_path = Path('.') / 'output'
    output_path.mkdir(exist_ok=True)
    workbook.save(output_path / filename)
    return workbook

@pytest.fixture()
def context():
    return TestContext()


@given('I create a submission',
       target_fixture='submission')
def create_submission(ingest_api:IngestApi, context):
    submission = ingest_api.create_submission()
    context.submission = submission
    return submission


@given('I create a new submission on the same project',
       target_fixture='new_submission')
def new_submission_in_project(submission,
                              ingest_api: IngestApi,
                              submitter: IngestSubmitter,
                              context):
    project_uuid = get_project_uuid(ingest_api, submission)
    submission2 = ingest_api.create_submission()
    submission2_url = submission2['_links']['self']['href']
    submitter.link_submission_to_project(project_uuid, submission2_url)
    context.new_submission = submission2
    return submission2


@then(parsers.parse("spreadsheet contains a file called {filename} with type {file_type}"))
def step_impl(spreadsheet: Workbook, filename, file_type: str):
    file_sheet_name = file_type.replace('_', ' ').capitalize()
    file_sheet: Worksheet = spreadsheet.get_sheet_by_name(file_sheet_name)

    filename_column = 2
    for row in file_sheet.iter_rows(min_row=6, max_col=filename_column, min_col=filename_column):
        for cell in row:
            if cell.value == filename:
                return
    pytest.fail(f'could not find {file_type} called {filename} in spreadsheet')


@given(parsers.parse("set submission state to {submissionState}"))
def step_impl1(ingest_api, submission, submissionState):
    stateChangeUrl = ingest_api.get_link_from_resource(submission, f'commit{submissionState}')
    ingest_api.put(stateChangeUrl)