import uuid

import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity


@pytest.fixture
def domain_type():
    return 'protocol'


@pytest.fixture
def concrete_type():
    return 'sequencing_protocol'

@pytest.fixture
def schema_url(domain_type, concrete_type):
    return f'https://schema.humancellatlas.org/type/{domain_type}/sequencing/10.1.0/{concrete_type}'


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
            'describedBy': schema_url
        },
        'uuid': {'uuid': metadata_uuid},
        "_links" : {
            "self" : {
                "href": f"https://api.ingest.archive.data.humancellatlas.org/projects/{metadata_id}"
            }
        }
    })


def test_populated_entity(populated_entity, metadata_uuid, metadata_id, schema_url, domain_type, concrete_type):
    assert_that(populated_entity).has_uuid(metadata_uuid).has_id(metadata_id)
    assert_that(populated_entity.schema).has_url(schema_url).has_domain_type(domain_type).has_concrete_type(concrete_type)


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
def entity_missing_schema(request) -> Entity:
    return Entity(request.param)


def test_entity_missing_schema(entity_missing_schema: Entity):
    assert_that(entity_missing_schema.schema).has_url('').has_domain_type('').has_concrete_type('')


@pytest.fixture(params=[
    pytest.lazy_fixture('schema_missing'),
    pytest.lazy_fixture('content_none'),
    pytest.lazy_fixture('content_missing')
])
def entity_missing_content(request) -> Entity:
    return Entity(request.param)


def test_entity_missing_content(entity_missing_content: Entity):
    assert_that(entity_missing_content).has_content({})


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
def entity_missing_uuid(request) -> Entity:
    return Entity(request.param)


def test_entity_missing_uuid(entity_missing_uuid: Entity):
    assert_that(entity_missing_uuid).has_uuid('')


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
def entity_missing_link(request) -> Entity:
    return Entity(request.param)


def test_entity_missing_link(entity_missing_link: Entity):
    assert_that(entity_missing_link).has_id('')
