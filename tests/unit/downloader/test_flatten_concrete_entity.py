import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity

from .conftest import get_json_file


@pytest.fixture(params=['entities', 'project-list', 'collection_protocol-list'])
def test_name(request):
    return request.param


@pytest.fixture
def json_list(script_dir, test_name):
    return get_json_file(script_dir + f'/{test_name}.json')


@pytest.fixture
def expected(script_dir, test_name):
    return get_json_file(script_dir + f'/{test_name}-flattened.json')


@pytest.fixture
def entity_list(json_list):
    return Entity.from_json_list(json_list)


def test_flatten_concrete_entity(flattener, entity_list, expected):
    # when
    actual = flattener.flatten(entity_list)
    # then
    assert_that(actual).is_equal_to(expected, ignore='Schemas')
    assert_that(set(actual.get('Schemas', []))).is_equal_to(set(expected.get('Schemas', [])))
