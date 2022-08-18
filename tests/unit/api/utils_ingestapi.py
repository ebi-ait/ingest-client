from unittest.mock import MagicMock, Mock

import requests_mock
from requests_cache import CachedSession
from requests_mock import Adapter, Mocker

from hca_ingest.api.ingestapi import IngestApi


@requests_mock.Mocker(kw='mock', case_sensitive=True)
def mocked_response_ingest_api(api_url='http://localhost:8080', **kwargs):
    mock = kwargs.get('mock')
    adapter = Adapter()
    token_manager = mock_token_manager()
    session = mock_cached_session(adapter, 'http://', 'https://')
    mock.get(api_url, json=get_api_root(api_url))
    api = IngestApi(url=api_url, token_manager=token_manager, session=session)
    adapter.reset()
    return api


def get_api_root(api_url):
    return {
        '_links': {
            'schemas': {
                'href': f'{api_url}/schemas'
            },
            'stagingJobs': {
                'href': f'{api_url}/stagingJobs'
            }
        }
    }


def mock_cached_session(adapter, *prefixes) -> CachedSession:
    # session will make mock requests where it would normally make real requests
    # the cache will only expire manually
    session = CachedSession(backend='memory')
    if not prefixes:
        session.mount('', adapter)
    else:
        for prefix in prefixes:
            session.mount(prefix, adapter)
    return session


def mock_token_manager():
    token_manager = MagicMock()
    token_manager.get_token = Mock(return_value='token')
    return token_manager


def register_schema_responses(mock: Mocker, api_url, page_size, schemas):
    schemas_url = f'{api_url}/schemas'
    schemas_get_url = f'{schemas_url}?size=1'
    search_url = f'{schemas_url}/search'
    latest_url = f'{search_url}/latestSchemas'
    latest_get_url = f'{latest_url}?size={page_size}'
    mock.get(schemas_get_url, json={
        '_links': {
            'search': {
                'href': search_url
            }
        }
    })
    mock.get(search_url, json={
        '_links': {
            'latestSchemas': {
                'href': latest_url
            }
        }
    })
    mock.get(latest_get_url, json={
        '_embedded': {
            'schemas': schemas
        },
        '_links': {}
    })
    return schemas_get_url, search_url, latest_get_url
