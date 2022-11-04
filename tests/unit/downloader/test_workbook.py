import uuid
from unittest import TestCase

from requests_mock import Mocker

from hca_ingest.downloader.workbook import WorkbookDownloader
from tests.unit.api.utils_ingestapi import mocked_response_ingest_api

API_URL = "https://mockingestdownload.com"


@Mocker(case_sensitive=True)
class TestWorkbookDownloader(TestCase):
    def setUp(self) -> None:
        self.mock_api = mocked_response_ingest_api(API_URL)

    def test_single_submission(self, mock: Mocker):
        # Given
        sub_uuid = str(uuid.uuid4())
        project_uuid = str(uuid.uuid4())
        self.register_submission_responses(mock, sub_uuid=sub_uuid, proj_uuid=project_uuid)

        # When
        downloader = WorkbookDownloader(self.mock_api)
        workbook = downloader.get_workbook_from_submission(sub_uuid)

        # Then
        uuids = []
        for row in workbook['Project'].iter_rows(min_row=6, min_col=1, max_col=1):
            uuids.append(row[0].value)
        self.assertListEqual([project_uuid], uuids)

    def test_second_submission_does_not_include_the_first(self, mock: Mocker):
        # Given
        first_sub_uuid = str(uuid.uuid4())
        first_project_uuid = str(uuid.uuid4())
        self.register_submission_responses(mock, sub_uuid=first_sub_uuid,
                                           proj_uuid=first_project_uuid)

        second_sub_uuid = str(uuid.uuid4())
        second_project_uuid = str(uuid.uuid4())
        self.register_submission_responses(mock, sub_uuid=second_sub_uuid,
                                           proj_uuid=second_project_uuid)

        # When
        downloader = WorkbookDownloader(self.mock_api)
        first_workbook = downloader.get_workbook_from_submission(first_sub_uuid)
        second_workbook = downloader.get_workbook_from_submission(second_sub_uuid)

        # Then
        first_uuids = []
        for row in first_workbook['Project'].iter_rows(min_row=6, min_col=1, max_col=1):
            first_uuids.append(row[0].value)
        self.assertListEqual([first_project_uuid], first_uuids)

        second_uuids = []
        for row in second_workbook['Project'].iter_rows(min_row=6, min_col=1, max_col=1):
            second_uuids.append(row[0].value)
        self.assertListEqual([second_project_uuid], second_uuids)

    def register_submission_responses(self, mock: Mocker, sub_id=str(uuid.uuid4()),
                                      sub_uuid=str(uuid.uuid4()), proj_uuid=str(uuid.uuid4())):
        sub_url, submission = self.register_item_responses(mock, 'submissionEnvelopes', sub_id,
                                                           sub_uuid)
        search_url = f'{API_URL}/submissionEnvelopes/search/findByUuidUuid?uuid={sub_uuid}'
        project_url = f"{sub_url}/relatedProjects"
        link_map_url = f"{sub_url}/linkingMap"
        submission['_links'].setdefault('relatedProjects', {})['href'] = project_url
        submission['_links'].setdefault('linkingMap', {})['href'] = link_map_url

        proj_url, project = self.register_item_responses(mock, 'projects', item_uuid=proj_uuid)
        mock.get(search_url, json=submission)
        mock.get(project_url, json=self.items_response('projects', [project]))
        mock.get(link_map_url, json=self.empty_link_map())

    def register_item_responses(self, mock: Mocker, folder: str, item_id=str(uuid.uuid4()),
                                item_uuid=str(uuid.uuid4())):
        folder_url = f'{API_URL}/{folder}'
        item_url = f'{folder_url}/{item_id}'
        search_url = f'{folder_url}/search/findByUuid?uuid={item_uuid}'
        singular = folder.removesuffix('es') if folder.endswith('es') else folder.removesuffix('s')
        item = {
            'content': {
                'describedBy': f'https://schema/type/{singular}/17.0.0/{singular}'
            },
            'uuid': {
                'uuid': item_uuid
            },
            '_links': {
                'self': {
                    'href': item_url
                },
            }
        }
        mock.get(item_url, json=item)
        mock.get(search_url, json=item)
        mock.get(folder_url, json=self.items_response(folder, [item]))

        return item_url, item

    @staticmethod
    def items_response(folder: str, items=[]):
        return {
            '_embedded': {
                f'{folder}': items
            }
        }

    @staticmethod
    def empty_link_map():
        return {
            'biomaterials': {},
            'files': {}
        }
