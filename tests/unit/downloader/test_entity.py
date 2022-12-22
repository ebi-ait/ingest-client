import uuid

import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity


@pytest.fixture
def schema_url():
    return 'https://schema.humancellatlas.org/type/project/17.0.0/project'


@pytest.fixture
def metadata_uuid():
    return str(uuid.uuid4())


@pytest.fixture
def metadata_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def populated_json(schema_url, metadata_uuid, metadata_id):
    return {
        'content': {
            'describedBy': schema_url
        },
        'uuid': {'uuid': metadata_uuid},
        "_links" : {
            "self" : {
                "href" : f"https://api.ingest.archive.data.humancellatlas.org/projects/{metadata_id}"
            }
        }
    }


@pytest.fixture
def populated_entity(populated_json):
    return Entity(populated_json)


def test_populated_entity(populated_entity, metadata_uuid, metadata_id, schema_url):
    assert_that(populated_entity.uuid).is_equal_to(metadata_uuid)
    assert_that(populated_entity.id).is_equal_to(metadata_id)
    assert_that(populated_entity.schema_url).is_equal_to(schema_url)


@pytest.fixture
def blank_entity():
    entity = {}
    return Entity(entity)


@pytest.fixture
def test_blank_partial_entity(blank_entity):
    assert_that(blank_entity.content).is_equal_to({})
    assert_that(blank_entity.uuid).is_equal_to('')
    assert_that(blank_entity.id).is_equal_to('')
    assert_that(blank_entity.schema_url).is_equal_to('')
