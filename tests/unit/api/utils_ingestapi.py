from requests_cache import CachedSession
from requests_mock import Adapter

from hca_ingest.api.ingestapi import IngestApi


def get_ingest_api_with_mocked_responses(api_url='http://localhost:8080', token_manager=None):
    adapter = Adapter()
    session = mock_cached_session(adapter, 'http://', 'https://')
    register_mock_response(
        adapter,
        api_url,
        get_api_root(api_url)
    )
    return adapter, IngestApi(url=api_url, token_manager=token_manager, session=session)


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


def register_mock_response(adapter, uri, response, method='GET', status_code=200):
    adapter.register_uri(
        method,
        uri,
        headers={'Content-Type': 'application/json'},
        json=response,
        status_code=status_code,
    )

