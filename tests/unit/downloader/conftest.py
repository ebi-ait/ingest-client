import json
import os

import pytest

from hca_ingest.downloader.flattener import Flattener
from hca_ingest.downloader.entity import Entity

@pytest.fixture
def flattener() -> Flattener:
    return Flattener()


@pytest.fixture
def script_dir() -> str:
    return os.path.dirname(__file__)


@pytest.fixture
def content(script_dir):
    return get_json_file(script_dir + '/content.json')


@pytest.fixture
def expected(script_dir):
    return get_json_file(script_dir + '/content-flattened.json')


@pytest.fixture
def metadata_uuid() -> str:
    return 'uuid1'


def get_json_file(filepath: str):
    with open(filepath) as file:
        return json.load(file)


def get_entity_file(filepath: str) -> Entity:
    return Entity(get_json_file(filepath))


def get_entities_file(filepath: str) -> list[Entity]:
    return Entity.from_json_list(get_json_file(filepath))
