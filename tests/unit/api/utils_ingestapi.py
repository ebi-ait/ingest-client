from requests_cache import CachedSession


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

