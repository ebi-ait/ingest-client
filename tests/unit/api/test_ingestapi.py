from unittest import TestCase
from unittest.mock import patch

import requests
from mock import MagicMock, Mock
from requests import HTTPError
from requests_mock import Adapter

from hca_ingest.api.ingestapi import IngestApi
from tests.unit.api.utils_ingestapi import mock_cached_session, register_mock_response

mock_ingest_api_url = "http://mockingestapi.com"
mock_submission_envelope_id = "mock-envelope-id"


class IngestApiTest(TestCase):
    def setUp(self):
        self.token_manager = MagicMock()
        self.token_manager.get_token = Mock(return_value='token')

    @patch('hca_ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    @patch('hca_ingest.api.ingestapi.requests.post')
    def test_create_file(self, mock_requests_post, mock_create_session, mock_get_link):
        ingest_api = IngestApi(token_manager=self.token_manager)
        mock_get_link.return_value = 'url/sub/id/files'
        mock_requests_post.return_value.json.return_value = {'uuid': 'file-uuid'}
        mock_requests_post.return_value.status_code = requests.codes.ok
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {})
        self.assertEqual(file, {'uuid': 'file-uuid'})
        mock_requests_post.assert_called_with('url/sub/id/files',
                                              headers={'Content-type': 'application/json',
                                                       'Authorization': 'Bearer token'},
                                              json={'fileName': 'mock-filename', 'content': {}},
                                              params={})

    @patch('hca_ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('hca_ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    @patch('hca_ingest.api.ingestapi.requests.post')
    def test_create_file_conflict(self, mock_requests_post, mock_create_session, mock_get_link, mock_get_file):
        ingest_api = IngestApi(token_manager=self.token_manager)
        mock_get_file.return_value = {
            '_embedded': {
                'files': [
                    {
                        'content': {'attr': 'value'},
                        '_links': {'self': {'href': 'existing-file-url'}}
                    }
                ]
            }
        }

        mock_get_link.return_value = 'url/sub/id/files'
        mock_create_session.return_value.patch.return_value.json.return_value = 'response'
        mock_requests_post.return_value.status_code = requests.codes.conflict
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename, {'attr2': 'value2'})
        self.assertEqual(file, 'response')
        mock_requests_post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json', 'Authorization': 'Bearer token'},
                                json={'content': {'attr': 'value', 'attr2': 'value2'}})

    @patch('hca_ingest.api.ingestapi.IngestApi.get_file_by_submission_url_and_filename')
    @patch('hca_ingest.api.ingestapi.IngestApi.get_link_in_submission')
    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    @patch('hca_ingest.api.ingestapi.requests.post')
    def test_create_file_internal_server_error(self, mock_requests_post, mock_create_session, mock_get_link,
                                               mock_get_file):
        ingest_api = IngestApi(token_manager=self.token_manager)
        mock_get_file.return_value = {
            '_embedded': {
                'files': [
                    {
                        'content': {'attr': 'value'},
                        '_links': {'self': {'href': 'existing-file-url'}}
                    }
                ]
            }
        }

        mock_get_link.return_value = 'url/sub/id/files'
        mock_create_session.return_value.patch.return_value.json.return_value = 'response'
        mock_requests_post.return_value.status_code = requests.codes.internal_server_error
        api_url = mock_ingest_api_url
        submission_id = mock_submission_envelope_id
        submission_url = api_url + "/" + submission_id
        filename = "mock-filename"

        file = ingest_api.create_file(submission_url, filename,
                                      {'attr2': 'value2'})
        self.assertEqual(file, 'response')
        mock_requests_post.assert_called_once()
        mock_create_session.return_value.patch \
            .assert_called_with('existing-file-url',
                                headers={'Content-type': 'application/json', 'Authorization': 'Bearer token'},
                                json={'content': {'attr': 'value',
                                                  'attr2': 'value2'}})

    @patch('hca_ingest.api.ingestapi.IngestApi.get_link_from_resource_url')
    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    def test_get_submission_by_uuid(self, mock_create_session, mock_get_link):
        mock_get_link.return_value = 'url/{?uuid}'
        ingest_api = IngestApi(token_manager=self.token_manager)
        mock_create_session.return_value.get.return_value.json.return_value = {'uuid': 'submission-uuid'}
        submission = ingest_api.get_submission_by_uuid('uuid')
        self.assertEqual(submission, {'uuid': 'submission-uuid'})

    def test_get_all(self):
        # given
        adapter = Adapter()
        api_url = 'http://test.api.get_all'
        session = mock_cached_session(adapter, 'http://', 'https://')
        register_mock_response(
            adapter,
            api_url,
            {'_links': {}}
        )
        register_mock_response(
            adapter,
            f'{api_url}?page=0&size=3',
            {
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
                        'href': f'{api_url}?page=1&size=3'
                    }
                }
            }
        )
        register_mock_response(
            adapter,
            f'{api_url}?page=1&size=3',
            {
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
        ingest_api = IngestApi(url=api_url, token_manager=self.token_manager, session=session)

        # when
        entities = ingest_api.get_all(f'{api_url}?page=0&size=3', "bundleManifests")
        self.assertEqual(len(list(entities)), 5)

    def test_get_related_entities_count(self):
        # given
        adapter = Adapter()
        api_url = 'http://test.api.get_related_entities_count'
        session = mock_cached_session(adapter, 'http://', 'https://')
        register_mock_response(
            adapter,
            api_url,
            {'_links': {}}
        )
        register_mock_response(
            adapter,
            f'{api_url}/project/files',
            {
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
        ingest_api = IngestApi(url=api_url, token_manager=self.token_manager, session=session)

        mock_entity = {
            "_links": {
                "self": {
                    "href": f'{api_url}/project/1'
                },
                "files": {
                    "href": f'{api_url}/project/files',
                }
            }
        }

        # when
        count = ingest_api.get_related_entities_count('files', mock_entity, 'files')

        # then
        self.assertEqual(count, 5)

    def test_get_related_entities_count_no_pagination(self):
        # given
        adapter = Adapter()
        api_url = 'http://test.api.get_related_entities_count_no_pagination'
        session = mock_cached_session(adapter, 'http://', 'https://')
        register_mock_response(
            adapter,
            api_url,
            {'_links': {}}
        )
        ingest_api = IngestApi(url=api_url, token_manager=self.token_manager, session=session)

        register_mock_response(
            adapter,
            f'{api_url}/project/files',
            {
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

        mock_entity = {
            "_links": {
                "self": {
                    "href": f'{api_url}/project/1'
                },
                "files": {
                    "href": f'{api_url}/project/files',
                }
            }
        }

        # when
        count = ingest_api.get_related_entities_count('files', mock_entity, 'files')
        self.assertEqual(count, 4)

    def test_create_staging_job_success(self):
        # given
        adapter = Adapter()
        api_url = 'http://test.api.create_staging_job_success'
        session = mock_cached_session(adapter, 'http://', 'https://')
        register_mock_response(
            adapter,
            api_url,
            {
                '_links': {
                    'stagingJobs': {
                        'href': f'{api_url}/stagingJobs'
                    }
                }
            }
        )
        register_mock_response(
            adapter,
            f'{api_url}/stagingJobs',
            {'staging-area-uuid': 'uuid'},
            method='POST'
        )
        ingest_api = IngestApi(url=api_url, token_manager=self.token_manager, session=session)

        # when
        staging_job = ingest_api.create_staging_job('uuid', 'filename', 'metadata-uuid')

        self.assertEqual(staging_job, {'staging-area-uuid': 'uuid'})

    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    @patch('hca_ingest.api.ingestapi.requests.post')
    def test_create_staging_job_failure(self, mock_post, mock_session):
        # given
        adapter = Adapter()
        api_url = 'http://test.api.create_staging_job_failure'
        session = mock_cached_session(adapter, 'http://', 'https://')
        register_mock_response(
            adapter,
            api_url,
            {
                '_links': {
                    'stagingJobs': {
                        'href': f'{api_url}/stagingJobs'
                    }
                }
            }
        )
        register_mock_response(
            adapter,
            f'{api_url}/stagingJobs',
            {'staging-area-uuid': 'uuid'},
            method='POST',
            status_code=500
        )
        ingest_api = IngestApi(url=api_url, token_manager=self.token_manager, session=session)

        # then
        with self.assertRaises(HTTPError):
            # when
            ingest_api.create_staging_job('uuid', 'filename', 'metadata_uuid')


    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    def test_get_latest_schema_url(self, mock_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)
        latest_schema_url = 'latest-project-schema-url'
        ingest_api.get_schemas = MagicMock(return_value=[{'_links': {'json-schema': {'href': latest_schema_url}}}])

        # when
        result = ingest_api.get_latest_schema_url('type', 'project', 'project')

        # then
        self.assertEqual(result, latest_schema_url)

    @patch('hca_ingest.api.ingestapi.create_session_with_retry')
    def test_get_latest_schema_url__empty_result(self, mock_session):
        # given
        ingest_api = IngestApi(token_manager=self.token_manager)
        ingest_api.get_schemas = MagicMock(return_value=[])

        # when
        result = ingest_api.get_latest_schema_url('type', 'project', 'project')

        # then
        self.assertEqual(result, None)