import uuid

from mock import MagicMock, Mock
from requests_cache import CachedSession
from requests_mock import Adapter
from unittest import TestCase

from hca_ingest.api.ingestapi import IngestApi


API_URL = "http://test.caching.ingest"
SCHEMAS_URL = f'{API_URL}/schemas'
SCHEMAS_RESPONSE = {
    '_links': {
        'schemas': {
            'href': SCHEMAS_URL
        }
    }
}


class IngestApiCacheTest(TestCase):
    def setUp(self):
        self.token_manager = MagicMock()
        self.token_manager.get_token = Mock(return_value='token')
        self.adapter = Adapter()
        self.register_mock_response('http://test.caching.ingest', SCHEMAS_RESPONSE)
        session = self.mock_session(self.adapter)
        self.api = IngestApi(url=API_URL, token_manager=self.token_manager, session=session)

    def tearDown(self):
        self.api.session.cache.clear()

    @staticmethod
    def mock_session(adapter):
        # session will make mock requests where it would normally make real requests
        session = CachedSession(backend='memory')
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def register_mock_response(self, uri, response, method='GET', status_code=200):
        self.adapter.register_uri(
            method,
            uri,
            headers={'Content-Type': 'application/json'},
            json=response,
            status_code=status_code,
        )

    def test_init_adds_to_cache(self):
        self.assertTrue(self.api.session.cache.has_url(API_URL))

    def test_schema_uses_cache(self):
        # given
        search_url = f'{SCHEMAS_URL}/search'
        high_level = 'foo'
        domain = 'barr'
        concrete = 'wool'
        mock_url = 'http://flibble'
        schemas_get_url, latest_get_url = self.register_schema_responses(
            search_url, high_level, domain, concrete, mock_url
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
        self.assertTrue(self.api.get(schemas_get_url).from_cache)
        self.assertTrue(self.api.get(search_url).from_cache)
        self.assertTrue(self.api.get(latest_get_url).from_cache)

    def test_patch_removes_item_from_cache(self):
        self.removes_item_from_cache(self.api.patch, patch={})

    def test_put_removes_item_from_cache(self):
        self.removes_item_from_cache(self.api.put)

    def test_post_removes_item_from_cache(self):
        self.removes_item_from_cache(self.api.post)

    def test_delete_removes_item_from_cache(self):
        self.removes_item_from_cache(self.api.delete)

    def test_new_item_clears_folder_cache(self):
        # given
        new_item = {
            'uuid': {
                'uuid': str(uuid.uuid4())
            }
        }
        folder_url, item_url, search_url, search_all_url = self.register_item_responses()
        self.register_mock_response(folder_url, new_item, method='POST', status_code=200)
        self.prime_cache(folder_url, item_url, search_url, search_all_url)

        # when
        self.api.post(folder_url, json=new_item)

        # then
        self.assertFalse(self.api.session.cache.has_url(folder_url))

    def removes_item_from_cache(self, action, **kwargs):
        # given
        folder_url, item_url, search_url, search_all_url = self.register_item_responses()
        self.prime_cache(folder_url, item_url, search_url, search_all_url)

        # when
        action(item_url, **kwargs)

        # then
        self.assertFalse(self.api.session.cache.has_url(folder_url))
        self.assertFalse(self.api.session.cache.has_url(item_url))
        self.assertFalse(self.api.session.cache.has_url(search_url))
        self.assertFalse(self.api.session.cache.has_url(search_all_url))

    def register_item_responses(self, item_type='mocks', item_id='id', item_uuid=str(uuid.uuid4())):
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
        self.register_mock_response(item_url, item)
        self.register_mock_response(item_url, item, method='PUT', status_code=200)
        self.register_mock_response(item_url, item, method='PATCH', status_code=200)
        self.register_mock_response(item_url, item, method='POST', status_code=200)
        self.register_mock_response(item_url, {}, method='DELETE', status_code=204)
        self.register_mock_response(search_url, item)
        self.register_mock_response(folder_url, folder)
        self.register_mock_response(search_all_url, folder)
        return folder_url, item_url, search_url, search_all_url

    def prime_cache(self, *urls):
        for url in urls:
            self.api.get(url)
            self.assertTrue(self.api.session.cache.has_url(url))

    def register_schema_responses(self, search_url, high_level, domain, concrete, mock_url):
        schemas_get_url = f'{SCHEMAS_URL}?size=1'
        latest_url = f'{search_url}/latestSchemas'
        latest_get_url = f'{latest_url}?size={self.api.page_size}'

        self.register_mock_response(schemas_get_url, {
            '_links': {
                'search': {
                    'href': search_url
                }
            }
        })
        self.register_mock_response(search_url, {
            '_links': {
                'latestSchemas': {
                    'href': latest_url
                }
            }
        })
        self.register_mock_response(latest_get_url, {
            '_embedded': {
                'schemas': [
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
            },
            '_links': {}
        })
        return schemas_get_url, latest_get_url
