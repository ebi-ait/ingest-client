# test spreadsheet export for project with more than a single submission.
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from assertpy import assert_that
from openpyxl.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from pytest_bdd import given, parsers, when, then, scenario

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.importer.importer import XlsImporter
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter


@scenario('multiple_submissions.feature', 'upload spreadsheet then add new file in a new submission using api')
def test_upload_and_add():
    pass


@scenario('multiple_submissions.feature', 'upload a spreadsheet then add and delete a file in first submission')
def test_upload_and_update_including_deletion():
    pass


@scenario('multiple_submissions.feature', 'export a production dataset')
def test_export_a_prod_dataset():
    pass


@scenario('multiple_submissions.feature', 'export a test dataset')
def test_export_a_test_dataset():
    pass


@scenario('multiple_submissions.feature', 'export a large test dataset')
def test_export_a_large_dataset():
    pass


@dataclass
class TestContext:
    first_submission: dict = None
    submission: dict = None
    project_uuid: str = ''

    def init_first_submission(self, submission):
        if self.first_submission is None:
            self.first_submission = submission


@given(parsers.parse('add a new file called {filename} with type {file_type}'))
def add_file(ingest_api: IngestApi, submission, filename, file_type):
    schema_url = ingest_api.get_latest_schema_url('type', 'file', file_type)
    spreadsheet_payload = build_file_payload(schema_url, filename)
    submission_url = ingest_api.get_link_from_resource(submission, 'self')
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


def get_project_uuid(ingest_api: IngestApi, submission):
    projects_url = ingest_api.get_link_from_resource(submission, 'projects')
    project_uuid = ingest_api.get(projects_url).json().get('_embedded', {}).get('projects', [])[0]['uuid']['uuid']
    return project_uuid


@given(parsers.parse('upload spreadsheet {filename}'))
def import_spreadsheet(filename: str,
                       submission,
                       ingest_api: IngestApi,
                       data_path: Path,
                       importer: XlsImporter):
    path = data_path / filename
    submission_url = ingest_api.get_link_from_resource(submission, 'self')
    imported_submission, template_manager = importer.import_file(path, submission_url)
    return imported_submission


@when('I export a spreadsheet',
      target_fixture='spreadsheet')
def export_to_spreadsheet(submission, downloader: WorkbookDownloader, output_path):
    submission_uuid = submission["uuid"]["uuid"]
    filename = f'{submission_uuid}.xlsx'
    workbook = downloader.get_workbook_from_submission(submission_uuid)
    workbook.save(output_path / filename)
    return workbook


@pytest.fixture()
def context():
    return TestContext()


@given('I create a submission',
       target_fixture='submission')
def create_submission(ingest_api: IngestApi, context: TestContext):
    submission = ingest_api.create_submission()
    if context:
        context.init_first_submission(submission)
        context.submission = submission
    return submission


@pytest.fixture()
def first_submission(context: TestContext):
    return context.first_submission


@pytest.fixture()
def last_submission(context: TestContext):
    return context.submission


@given('I create a new submission on the same project',
       target_fixture='submission')
def new_submission_in_project(submission,
                              ingest_api: IngestApi,
                              submitter: IngestSubmitter,
                              context: TestContext):
    project_uuid = get_project_uuid(ingest_api, submission)
    submission2 = ingest_api.create_submission()
    submission2_url = ingest_api.get_link_from_resource(submission2, 'self')
    submitter.link_submission_to_project(project_uuid, submission2_url)
    context.submission = submission2
    return submission2


@then(parsers.parse("spreadsheet contains a file called {filename} with type {file_type}"))
def check_file_in_sptreadsheet(spreadsheet: Workbook, filename, file_type: str):
    if not check_spreadsheet_contains_filename(spreadsheet, filename, file_type):
        pytest.fail(f'could not find {file_type} called {filename} in spreadsheet')


@then("spreadsheet contains all files in submission")
def check_all_files_in_spreadsheet(spreadsheet: Workbook, submission, ingest_api: IngestApi):
    submission_files_url = ingest_api.get_link_from_resource(submission, 'files')
    for file in ingest_api.get_all(submission_files_url, entity_type='files'):
        filename = file['content']['file_core']['file_name']
        file_type = file['content']['describedBy'].split('/')[-1]
        if not check_spreadsheet_contains_filename(spreadsheet, filename, file_type):
            pytest.fail(f'could not find {file_type} called {filename} in spreadsheet')


def check_spreadsheet_contains_filename(spreadsheet, filename, file_type):
    file_sheet_name = file_type.replace('_', ' ').capitalize()
    file_sheet: Worksheet = spreadsheet.get_sheet_by_name(file_sheet_name)
    filename_column = 2
    found = False
    for row in file_sheet.iter_rows(min_row=6, max_col=filename_column, min_col=filename_column):
        for cell in row:
            if cell.value == filename:
                found = True
                break
    return found


@given(parsers.parse("set submission state to {submissionState}"))
def set_submission_state(ingest_api, submission, submissionState):
    stateChangeUrl = ingest_api.get_link_from_resource(submission, f'commit{submissionState}')
    ingest_api.put(stateChangeUrl)


@given(parsers.parse("delete a file called {filename} in first submission"))
def delete_file(context, filename, ingest_api: IngestApi):
    submission_url = ingest_api.get_link_from_resource(context.first_submission, 'self')
    files_resource = ingest_api.get_file_by_submission_url_and_filename(submission_url, filename)
    if len(files_resource) == 0:
        raise ValueError(f'file {filename} not found in submission {submission_url}')
    file_url = ingest_api.get_link_from_resource(files_resource[0], 'self')
    ingest_api.delete(file_url)


@then(parsers.parse("spreadsheet does not contain a file called {filename} with type {file_type}"))
def check_file_not_in_spreadsheet(spreadsheet, filename, file_type):
    if check_spreadsheet_contains_filename(spreadsheet, filename, file_type):
        pytest.fail(f'unexpected {file_type} called {filename} in spreadsheet')


@given(parsers.parse("submission with uuid {submission_uuid}"),
       target_fixture='submission')
def find_submission_by_uuid(ingest_api, submission_uuid):
    return ingest_api.get_submission_by_uuid(submission_uuid)


@then("spreadsheet is found in the output directory")
def check_spreadsheet_exists(output_path, submission):
    submission_uuid = submission["uuid"]["uuid"]
    filename = f'{submission_uuid}.xlsx'
    spreadsheet_path: Path = output_path / filename
    assert_that(str(spreadsheet_path)).exists()
