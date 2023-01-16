import uuid
from unittest import TestCase

import requests_mock
from requests_mock import Mocker

from tests.unit.api.utils_ingestapi import mocked_response_ingest_api, register_schema_responses

API_URL = "http://test.caching.ingest"


@requests_mock.Mocker(case_sensitive=True)
class IngestApiCacheTest(TestCase):
    def setUp(self):
        self.api = mocked_response_ingest_api(API_URL)

    def tearDown(self):
        self.api.session.cache.clear()

    def test_init_adds_to_cache(self, mock):
        self.assertTrue(self.api.session.cache.has_url(self.api.url))

    def test_schema_uses_cache(self, mock: Mocker):
        # given
        high_level = 'foo'
        domain = 'barr'
        concrete = 'wool'
        mock_url = 'http://flibble'
        schemas = [
            {
                'highLevelEntity': f'not-{high_level}',
            },
            {
                'highLevelEntity': high_level,
                'domainEntity': f'not-{domain}',
            },
            {
                'highLevelEntity': high_level,
                'domainEntity': domain,
                'concreteEntity': f'not-{concrete}'
            },
            {
                'highLevelEntity': high_level,
                'domainEntity': domain,
                'concreteEntity': concrete,
                '_links': {
                    'json-schema': {
                        'href': mock_url
                    }
                }
            },
            {
                'highLevelEntity': 'type',
                'domainEntity': 'project',
                'concreteEntity': 'project',
                '_links': {
                    'json-schema': {
                        'href': 'ignored'
                    }
                }
            }
        ]
        schemas_get_url, search_url, latest_get_url = register_schema_responses(
            mock, self.api.url, self.api.page_size, schemas
        )

        # and given
        self.assertFalse(self.api.session.cache.has_url(schemas_get_url))
        self.assertFalse(self.api.session.cache.has_url(search_url))
        self.assertFalse(self.api.session.cache.has_url(latest_get_url))

        # when
        result = self.api.get_latest_schema_url(high_level, domain, concrete)

        # then
        self.assertEqual(result, mock_url)
        self.assertTrue(self.api.session.cache.has_url(schemas_get_url))
        self.assertTrue(self.api.session.cache.has_url(search_url))
        self.assertTrue(self.api.session.cache.has_url(latest_get_url))

        # and then
        call_count = mock.call_count
        self.assertTrue(self.api.get(schemas_get_url).from_cache)
        self.assertTrue(self.api.get(search_url).from_cache)
        self.assertTrue(self.api.get(latest_get_url).from_cache)
        self.assertEqual(call_count, mock.call_count)

    def test_patch_removes_item_from_cache(self, mock):
        self.removes_item_from_cache(mock, self.api.patch, json={})

    def test_put_removes_item_from_cache(self, mock):
        self.removes_item_from_cache(mock, self.api.put)

    def test_post_removes_item_from_cache(self, mock):
        self.removes_item_from_cache(mock, self.api.post)

    def test_delete_removes_item_from_cache(self, mock):
        self.removes_item_from_cache(mock, self.api.delete)

    def test_new_item_clears_folder_cache(self, mock):
        # given
        new_item = {
            'uuid': {
                'uuid': str(uuid.uuid4())
            }
        }
        folder_url, item_url, search_url, search_all_url = self.register_item_responses(mock)
        mock.post(folder_url, json=new_item)
        self.prime_cache(folder_url, item_url, search_url, search_all_url)

        # when
        self.api.post(folder_url, json=new_item)

        # then
        self.assertFalse(self.api.session.cache.has_url(folder_url))

    def test_bypass_cache_updates_cache(self, mock):
        # given
        test_id = str(uuid.uuid4()).replace('-', '')
        test_url = f'{API_URL}/BypassTest/{test_id}'
        test_case_responses = [
            {
                'json': {
                    'editable': False,
                }
            },
            {
                'json': {
                    'editable': True,
                }
            }
        ]
        mock.get(test_url, test_case_responses)

        # first_response
        self.assertFalse(self.api.is_response_editable(self.api.get(test_url)))
        self.assertEqual(mock.call_count, 1)

        # cached_response still false
        self.assertFalse(self.api.is_response_editable(self.api.get(test_url)))
        self.assertEqual(mock.call_count, 1)

        # bypass_cache
        self.assertTrue(self.api.is_response_editable(self.api.get(test_url, bypass_cache=True)))
        self.assertEqual(mock.call_count, 2)

        # then cached result is updated
        self.assertTrue(self.api.is_response_editable(self.api.get(test_url)))
        self.assertEqual(mock.call_count, 2)

    def test_polling(self, mock):
        # given
        test_id = str(uuid.uuid4()).replace('-', '')
        test_url = f'{API_URL}/BypassTest/{test_id}'
        test_case_responses = [
            {
                'json': {
                    'editable': False,
                }
            },
            {
                'json': {
                    'editable': True,
                }
            }
        ]
        mock.get(test_url, test_case_responses)

        # when
        self.api.poll(test_url, step=1, max_tries=2, check_success=self.api.is_response_editable)
        self.assertEqual(mock.call_count, 2)

    def removes_item_from_cache(self, mock: Mocker, action, **kwargs):
        # given
        folder_url, item_url, search_url, search_all_url = self.register_item_responses(mock)
        self.prime_cache(folder_url, item_url, search_url, search_all_url)

        # when
        action(item_url, **kwargs)

        # then
        self.assertFalse(self.api.session.cache.has_url(folder_url))
        self.assertFalse(self.api.session.cache.has_url(item_url))
        self.assertFalse(self.api.session.cache.has_url(search_url))
        self.assertFalse(self.api.session.cache.has_url(search_all_url))

    @staticmethod
    def register_item_responses(mock: Mocker, item_type='mocks', item_id='id', item_uuid=str(uuid.uuid4())):
        folder_url = f'{API_URL}/{item_type}'
        item_url = f'{folder_url}/{item_id}'
        search_url = f'{folder_url}/search/findByUuid?uuid={item_uuid}'
        search_all_url = f'{folder_url}/search/findAllByUuid?uuid={item_uuid}'

        item = {
            'uuid': {
                'uuid': item_uuid
            },
            '_links': {
                'self': {
                    'href': item_url
                }
            }
        }
        folder = {
            '_embedded': {
                'item_type': [
                    item
                ]
            }
        }
        mock.get(item_url, json=item)
        mock.put(item_url, json=item, status_code=200)
        mock.patch(item_url, json=item, status_code=200)
        mock.post(item_url, json=item, status_code=200)
        mock.delete(item_url, json={}, status_code=204)
        mock.get(search_url, json=item)
        mock.get(folder_url, json=folder)
        mock.get(search_all_url, json=folder)
        return folder_url, item_url, search_url, search_all_url

    def prime_cache(self, *urls):
        for url in urls:
            self.api.get(url)
            self.assertTrue(self.api.session.cache.has_url(url))
