import pytest
from assertpy import assert_that

import requests_mock

from hca_ingest.downloader.schema_collector import SchemaCollector
from hca_ingest.downloader.schema_url import SchemaUrl

from .conftest import get_json_file


@pytest.fixture
def schema_cache(script_dir):
    return get_json_file(script_dir + '/schema_cache.json')


@pytest.fixture
def schema_collector(schema_cache):
    schema_collector = SchemaCollector()
    schema_collector.schema_cache.update(schema_cache)
    return schema_collector


@pytest.fixture
def url():
    return 'https://schema.humancellatlas.org/type/protocol/biomaterial_collection/9.2.0/collection_protocol'


@pytest.fixture
def schema_url(url):
    return SchemaUrl(url)


@pytest.fixture
def expected_schemas(schema_cache, url):
    return {url: schema_cache[url]}


def test_collector_gets_schema_from_cache(schema_collector, schema_url, expected_schemas):
    with requests_mock.Mocker() as mock:
        schemas = schema_collector.get_schemas({schema_url})
        assert_that(schemas).is_equal_to(expected_schemas)
        assert_that(mock.call_count).is_zero()


def test_collector_gets_schema_from_internet(schema_collector, schema_cache, schema_url, expected_schemas):
    with requests_mock.Mocker() as mock:
        schema_collector.schema_cache.clear()
        for url, schema in schema_cache.items():
            mock.get(url, json=schema)
        schemas = schema_collector.get_schemas({schema_url})
        assert_that(schemas).is_equal_to(expected_schemas)
        assert_that(schema_collector.schema_cache).is_equal_to(schema_cache)
        assert_that(mock.call_count).is_equal_to(len(schema_cache.keys()))


def test_collector_gets_schema_from_cache_and_internet(schema_collector, schema_url, expected_schemas):
    with requests_mock.Mocker() as mock:
        for url, schema in expected_schemas.items():
            schema_collector.schema_cache.pop(url)
            mock.get(url, json=schema)
        schemas = schema_collector.get_schemas({schema_url})
        assert_that(schemas).is_equal_to(expected_schemas)
        assert_that(mock.call_count).is_equal_to(1)

        schema_collector.get_schemas({schema_url})
        assert_that(mock.call_count).is_equal_to(1)


@pytest.fixture
def multiple_schema_versions_of_same_concrete_entity() -> set[SchemaUrl]:
    return {
        SchemaUrl("https://schema.humancellatlas.org/type/biomaterial/14.2.0/donor_organism"),
        SchemaUrl("https://schema.humancellatlas.org/type/biomaterial/14.3.0/donor_organism")
    }



def test_collection_raises_error(multiple_schema_versions_of_same_concrete_entity, schema_collector):
    with pytest.raises(ValueError) as value_error:
        schema_collector.get_schemas(multiple_schema_versions_of_same_concrete_entity)
        assert_that(str(value_error.value)).is_equal_to("Multiple versions of same concrete entity schema")


@pytest.fixture
def duplicate_concrete_type_schema_urls():
    return [
        SchemaUrl('https://schema/type/domainA/version1/concreteA'),
        SchemaUrl('https://schema/type/domainB/version1/concreteB'),
        SchemaUrl('https://schema/type/domainC/version1/concreteC'),
        SchemaUrl('https://schema/type/domainD/version1/concreteD'),
        SchemaUrl('https://schema/type/domainA/version1/concreteA'),
        SchemaUrl('https://schema/type/domainB/version1/concreteB'),
        SchemaUrl('https://schema/type/domainC/version1/concreteC'),
        SchemaUrl('https://schema/type/domainD/version1/concreteD'),
        SchemaUrl('https://schema/type/domainA/version2/concreteA'),
        SchemaUrl('https://schema/type/domainD/version2/concreteD')
    ]


@pytest.fixture
def expected_duplicates():
    return {
        SchemaUrl('https://schema/type/domainA/version1/concreteA'),
        SchemaUrl('https://schema/type/domainD/version1/concreteD'),
        SchemaUrl('https://schema/type/domainA/version2/concreteA'),
        SchemaUrl('https://schema/type/domainD/version2/concreteD')
    }


def test_get_duplicate_schemas_by_concrete_type(duplicate_concrete_type_schema_urls, schema_collector, expected_duplicates):
    assert_that(duplicate_concrete_type_schema_urls).is_length(10)
    schema_urls = set(duplicate_concrete_type_schema_urls)
    assert_that(schema_urls).is_length(6)
    duplicate_uls = schema_collector.get_duplicate_schemas_by_concrete_type(schema_urls)
    assert_that(duplicate_uls).is_equal_to(expected_duplicates)
