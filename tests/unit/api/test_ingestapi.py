import uuid
from unittest import TestCase

import requests_mock
from requests import HTTPError
from requests_mock import Mocker

from tests.unit.api.utils_ingestapi import mocked_response_ingest_api, register_schema_responses

API_URL = "http://mockingestapi.com"


@requests_mock.Mocker(case_sensitive=True)
class IngestApiTest(TestCase):
    def setUp(self):
        self.api = mocked_response_ingest_api(API_URL)

    @staticmethod
    def mock_parent_child_link(mock: Mocker, parent_type, parent_id, child_type):
        parent_url = f'{API_URL}/{parent_type}/{parent_id}'
        child_url = f'{parent_url}/{child_type}'
        IngestApiTest.mock_link(mock, parent_url, child_type, child_url)
        return child_url, parent_url

    @staticmethod
    def mock_search_link(mock, parent_type, link_name, results=[], results_type=None, link_url=None):
        search_url = f'{API_URL}/{parent_type}/search'
        if not link_url:
            link_url = f'{search_url}/{link_name}'
        IngestApiTest.mock_link(mock, search_url, link_name, link_url)
        if not results_type:
            results_type = parent_type
        if results and results_type:
            results = {'_embedded': {results_type: results}}
            mock.get(link_url, json=results)

    @staticmethod
    def mock_link(mock, source_url, link_name, link_url):
        link_response = {
            '_links': {
                link_name: {
                    'href': link_url
                }
            }
        }
        mock.get(source_url, json=link_response)

    def test_create_file(self, mock):
        # given
        submission_files_url, submission_url = self.mock_parent_child_link(
            mock, 'submissionEnvelope', '1', 'files'
        )
        mock.post(submission_files_url, json={})

        filename = "test-filename"
        content = {'attribute': 'value'}
        target_request = {'fileName': filename, 'content': content}

        # when
        self.api.create_file(submission_url, filename, content)

        # then
        self.assertEqual(mock.call_count, 2)
        self.assertEqual(mock.last_request.path, submission_files_url.removeprefix(API_URL))
        self.assertDictEqual(mock.last_request.json(), target_request)

    def test_create_file_conflict(self, mock):
        self.create_file_fail_scenario(mock, 409)

    def test_create_file_internal_server_error(self, mock):
        self.create_file_fail_scenario(mock, 500)

    def create_file_fail_scenario(self, mock, status_code):
        # Given
        existing_file_url = f'{API_URL}/files/existing-file'
        existing_file_content = {'attr': 'value'}
        existing_file = {
            'content': existing_file_content,
            '_links': {'self': {'href': existing_file_url}}
        }
        new_file_content = {'attr2': 'value2'}
        target_content = {}
        target_content.update(existing_file_content)
        target_content.update(new_file_content)
        target = {'content': target_content}

        # Mocks
        submission_files_url, submission_url = self.mock_parent_child_link(
            mock, 'envelope', 'ENVELOPE_2', 'files'
        )
        mock.post(submission_files_url, status_code=status_code)
        self.mock_search_link(
            mock,
            parent_type='files',
            link_name='findBySubmissionEnvelopeAndFileName',
            results=[existing_file]
        )
        mock.patch(existing_file_url, json={})

        # when
        self.api.create_file(submission_url, "mock-filename", new_file_content)

        # then
        self.assertEqual(mock.call_count, 5)
        self.assertEqual(mock.last_request.path, existing_file_url.removeprefix(API_URL))
        self.assertDictEqual(mock.last_request.json(), target)

    def test_get_submission_by_uuid(self, mock):
        # given
        test_uuid = str(uuid.uuid4())
        target_submission = {'uuid': test_uuid}
        self.mock_search_link(mock, 'submissionEnvelopes', 'findByUuid', [target_submission])
        # when
        self.api.get_submission_by_uuid(test_uuid)
        # given
        self.assertEqual(mock.last_request.query, f'uuid={test_uuid}')

    def test_get_all(self, mock):
        # given
        url = f'{API_URL}/bundleManifests'
        mock.get(
            f'{url}?page=0&size=3',
            json={
                "page": {
                    "size": 3,
                    "totalElements": 5,
                    "totalPages": 2,
                    "number": 0
                },
                "_embedded": {
                    "bundleManifests": [
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                    "next": {
                        'href': f'{url}?page=1&size=3'
                    }
                }
            }
        )
        mock.get(
            f'{url}?page=1&size=3',
            json={
                "page": {
                    "size": 3,
                    "totalElements": 5,
                    "totalPages": 2,
                    "number": 1
                },
                "_embedded": {
                    "bundleManifests": [
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                }
            }
        )

        # when
        entities = self.api.get_all(f'{url}?page=0&size=3', "bundleManifests")

        # then
        self.assertEqual(len(list(entities)), 5)

    def test_get_related_entities_count(self, mock):
        # given
        project_files_url = f'{API_URL}/project/1/files'
        first_project_url = f'{API_URL}/project/1'
        mock.get(
            project_files_url,
            json={
                "page": {
                    "size": 3,
                    "totalElements": 5,
                    "totalPages": 2,
                    "number": 1
                },
                "_embedded": {
                    "files": [
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {
                }
            }
        )

        # when
        test_entity = {
            "_links": {
                "self": {
                    "href": first_project_url
                },
                "files": {
                    "href": project_files_url,
                }
            }
        }
        count = self.api.get_related_entities_count('files', test_entity, 'files')

        # then
        self.assertEqual(count, 5)

    def test_get_related_entities_count_no_pagination(self, mock):
        # given
        project_files_url = f'{API_URL}/project/1/files'
        first_project_url = f'{API_URL}/project/1'
        mock.get(
            project_files_url,
            json={
                "_embedded": {
                    "files": [
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"},
                        {"attr": "value"}
                    ]
                },
                "_links": {}
            }
        )

        # when
        test_entity = {
            "_links": {
                "self": {
                    "href": first_project_url
                },
                "files": {
                    "href": project_files_url,
                }
            }
        }
        count = self.api.get_related_entities_count('files', test_entity, 'files')

        # then
        self.assertEqual(count, 4)

    def test_create_staging_job_success(self, mock):
        # given
        staging_area_uuid = str(uuid.uuid4())
        metadata_uuid = str(uuid.uuid4())
        file_name = 'filename'
        target = {
            "stagingAreaUuid": staging_area_uuid,
            "stagingAreaFileName": file_name,
            "metadataUuid": metadata_uuid
        }
        mock.post(f'{API_URL}/stagingJobs', json={})
        # when
        self.api.create_staging_job(staging_area_uuid, file_name, metadata_uuid)
        # then
        self.assertDictEqual(mock.last_request.json(), target)

    def test_create_staging_job_failure(self, mock):
        # given
        mock.post(f'{API_URL}/stagingJobs', status_code=500)

        # then
        with self.assertRaises(HTTPError):
            # when
            self.api.create_staging_job('uuid', 'filename', 'metadata_uuid')

    def test_get_latest_schema_url(self, mock):
        # given
        latest_schema_url = 'latest-project-schema-url'
        schemas = [{
                'highLevelEntity': 'type',
                'domainEntity': 'project',
                'concreteEntity': 'project',
                '_links': {
                    'json-schema': {
                        'href': latest_schema_url
                    }
                }
            }]
        register_schema_responses(
            mock, self.api.url, self.api.page_size, schemas
        )
        # when
        result = self.api.get_latest_schema_url('type', 'project', 'project')

        # then
        self.assertEqual(result, latest_schema_url)
        self.assertEqual(mock.call_count, 3)

    def test_get_latest_schema_url__empty_result(self, mock):
        # given
        schemas = []
        register_schema_responses(
            mock, self.api.url, self.api.page_size, schemas
        )
        # when
        result = self.api.get_latest_schema_url('type', 'project', 'project')

        # then
        self.assertEqual(result, None)
