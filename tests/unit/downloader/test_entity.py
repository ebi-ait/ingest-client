import uuid

import pytest
from assertpy import assert_that

from downloader.schema_url import SchemaUrl
from hca_ingest.downloader.entity import Entity


@pytest.fixture
def domain_type():
    return 'protocol'


@pytest.fixture
def concrete_type():
    return 'sequencing_protocol'

@pytest.fixture
def schema_url(domain_type, concrete_type):
    return SchemaUrl(f'https://schema.humancellatlas.org/type/{domain_type}/sequencing/10.1.0/{concrete_type}')


@pytest.fixture
def metadata_uuid():
    return str(uuid.uuid4())


@pytest.fixture
def metadata_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def populated_entity(schema_url, metadata_uuid, metadata_id):
    return Entity({
        'content': {
            'describedBy': schema_url.url
        },
        'uuid': {'uuid': metadata_uuid},
        "_links" : {
            "self" : {
                "href": f"https://api.ingest.archive.data.humancellatlas.org/projects/{metadata_id}"
            }
        }
    })


def test_populated_entity(populated_entity, metadata_uuid, metadata_id, schema_url):
    assert_that(populated_entity.uuid).is_equal_to(metadata_uuid)
    assert_that(populated_entity.id).is_equal_to(metadata_id)
    assert_that(populated_entity.schema_url).is_equal_to(schema_url)


@pytest.fixture
def schema_empty_string():
    return {
        'content': {
            'describedBy': ''
        }
    }

@pytest.fixture
def schema_none():
    return {
        'content': {
            'describedBy': None
        }
    }

@pytest.fixture
def schema_missing():
    return {
        'content': {}
    }


@pytest.fixture
def content_none():
    return {
        'content': None
    }


@pytest.fixture
def content_missing():
    return {}


@pytest.fixture(params=[
    pytest.lazy_fixture('schema_empty_string'),
    pytest.lazy_fixture('schema_none'),
    pytest.lazy_fixture('schema_missing'),
    pytest.lazy_fixture('content_none'),
    pytest.lazy_fixture('content_missing')
])
def missing_schema_entity(request):
    return Entity(request.param)


@pytest.fixture
def missing_schema(missing_schema_entity: Entity) -> SchemaUrl:
    return missing_schema_entity.schema_url


@pytest.fixture
def blank_schema() -> SchemaUrl:
    return SchemaUrl()


def test_missing_schema(missing_schema: SchemaUrl, blank_schema: SchemaUrl):
    assert_that(missing_schema.url).is_equal_to(blank_schema.url)
    assert_that(missing_schema.concrete_type).is_equal_to(blank_schema.concrete_type)
    assert_that(missing_schema.domain_type).is_equal_to(blank_schema.domain_type)


@pytest.fixture(params=[
    pytest.lazy_fixture('schema_missing'),
    pytest.lazy_fixture('content_none'),
    pytest.lazy_fixture('content_missing')
])
def missing_content_entity(request):
    return Entity(request.param)


def test_missing_content(missing_content_entity):
    assert_that(missing_content_entity.content).is_equal_to({})


@pytest.fixture
def uuid_uuid_empty_string():
    return {
        'uuid': {'uuid': ''}
    }


@pytest.fixture
def uuid_uuid_none():
    return {
        'uuid': {'uuid': None},
    }


@pytest.fixture
def uuid_uuid_missing():
    return {
        'uuid': {}
    }


@pytest.fixture
def uuid_none():
    return {
        'uuid': None
    }


@pytest.fixture
def uuid_missing():
    return {}

@pytest.fixture(params=[
    pytest.lazy_fixture('uuid_uuid_empty_string'),
    pytest.lazy_fixture('uuid_uuid_none'),
    pytest.lazy_fixture('uuid_uuid_missing'),
    pytest.lazy_fixture('uuid_none'),
    pytest.lazy_fixture('uuid_missing')
])
def missing_uuid_entity(request):
    return Entity(request.param)


def test_missing_uuid(missing_uuid_entity):
    assert_that(missing_uuid_entity.uuid).is_equal_to('')


@pytest.fixture
def href_no_slash():
    return {
        "_links" : {
            "self" : {
                "href": "linkwithoutslash"
            }
        }
    }


@pytest.fixture
def href_empty_string():
    return {
        "_links" : {
            "self" : {
                "href": ""
            }
        }
    }


@pytest.fixture
def href_none():
    return {
        "_links" : {
            "self" : {
                "href": None
            }
        }
    }


@pytest.fixture
def href_missing():
    return {
        "_links" : {
            "self" : {}
        }
    }


@pytest.fixture
def self_none():
    return {
        "_links": {
            "self": None
        }
    }


@pytest.fixture
def self_missing():
    return {
        "_links": {}
    }


@pytest.fixture
def links_none():
    return {
        "_links": None
    }


@pytest.fixture
def links_missing():
    return {}


@pytest.fixture(params=[
    pytest.lazy_fixture('href_no_slash'),
    pytest.lazy_fixture('href_empty_string'),
    pytest.lazy_fixture('href_none'),
    pytest.lazy_fixture('href_missing'),
    pytest.lazy_fixture('self_none'),
    pytest.lazy_fixture('self_missing'),
    pytest.lazy_fixture('links_none'),
    pytest.lazy_fixture('links_missing')
])
def missing_link_entity(request):
    return Entity(request.param)


def test_missing_link(missing_link_entity):
    assert_that(missing_link_entity.id).is_equal_to('')
