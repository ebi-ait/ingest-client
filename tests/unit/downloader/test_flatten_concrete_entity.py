import json
import os
import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity
from hca_ingest.downloader.flattener import Flattener

@pytest.fixture
def script_dir():
    return os.path.dirname(__file__)


def get_json_file(filepath: str):
    with open(filepath) as file:
        return json.load(file)


@pytest.fixture
def flattener():
    return Flattener()

@pytest.fixture(params=['entities', 'project-list', 'collection_protocol-list'])
def test_name(request):
    return request.param


@pytest.fixture
def json_list(script_dir, test_name):
    return get_json_file(script_dir + f'/{test_name}.json')


@pytest.fixture
def entity_list(json_list):
    return Entity.from_json_list(json_list)


@pytest.fixture
def expected(script_dir, test_name):
    return get_json_file(script_dir + f'/{test_name}-flattened.json')


def test_flatten_concrete_entity(flattener, entity_list, expected):
    # when
    actual = flattener.flatten(entity_list)
    # then
    assert_that(actual).is_equal_to(expected)
