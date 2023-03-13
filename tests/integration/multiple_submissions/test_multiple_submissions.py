# test spreadsheet export for project with more than a single submission.
import dataclasses
import os
from pathlib import Path
from typing import List

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.importer.importer import XlsImporter

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
def test_generate_spreadsheet():
    # upload submission 1
    ingest_api = IngestApi()
    ingest_api.token = os.getenv('INGEST_TOKEN')
    downloader = WorkbookDownloader(ingest_api)
    importer = XlsImporter(ingest_api)
    data_path = Path(__file__).parent / 'data'
    filename='submission1.xlsx'
    submission_uuid = create_submission(filename, data_path, importer, ingest_api)
    export_to_spreadsheet(filename, submission_uuid, downloader)
    submission = ingest_api.get_submission_by_uuid(submission_uuid)
    project_uuid = get_project_uuid(ingest_api, submission)
    exported_url = ingest_api.get_link_from_resource(submission, 'commitExported')
    ingest_api.put(exported_url)
    # upload submission 2
    filename='submission2.xlsx'
    submission_uuid = create_submission(filename, data_path, importer, ingest_api, project_uuid=project_uuid)
    export_to_spreadsheet(filename, submission_uuid, downloader)


def get_project_uuid(ingest_api, submission):
    projects_url = ingest_api.get_link_from_resource(submission, 'projects')
    project_uuid = ingest_api.get(projects_url).json().get('_embedded', {}).get('projects', [])[0]['uuid']['uuid']
    return project_uuid


def create_submission(filename, data_path: Path, importer: XlsImporter, ingest_api: IngestApi, project_uuid=None):
    submission = ingest_api.create_submission()
    submission_url: str = submission["_links"]["self"]["href"]
    submission_uuid: str = submission["uuid"]["uuid"]
    path = data_path / filename
    imported_submission, template_manager = importer.import_file(path, submission_url, project_uuid=project_uuid, update_project=False)
    return submission_uuid


def export_to_spreadsheet(filename, submission_uuid, downloader: WorkbookDownloader):
    workbook = downloader.get_workbook_from_submission(submission_uuid)
    output_path = Path('.') / 'output'
    output_path.mkdir(exist_ok=True)
    workbook.save(output_path / filename)