import pytest
from assertpy import assert_that

import requests_mock

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.downloader.data_collector import DataCollector
from hca_ingest.downloader.flattener import Flattener
from hca_ingest.downloader.schema_collector import SchemaCollector
from hca_ingest.downloader.entity import Entity

from .conftest import get_json_file


@pytest.fixture
def small_project_mock(script_dir):
    mocker = requests_mock.Mocker()
    calls = get_json_file(script_dir + '/small-project-calls.json')
    for url, response in calls.items():
        mocker.get(url, json=response)
    return mocker


@pytest.fixture
def small_project_uuid():
    return 'ea837803-c5fb-4e11-93b9-374c6e5c645c'


@pytest.fixture
def submission_entities(small_project_mock, small_project_uuid) -> list[Entity]:
    with small_project_mock:
        api = IngestApi('https://api.ingest.staging.archive.data.humancellatlas.org')
        data_collector = DataCollector(api)
        submission = data_collector.collect_data_by_submission_uuid(small_project_uuid)
        return [entity for entity in submission.values() if entity.content]


@pytest.fixture
def schemas(small_project_mock, submission_entities) -> dict:
    with small_project_mock:
        schema_collector = SchemaCollector()
        return  schema_collector.get_schemas_for_entities(submission_entities)


@pytest.fixture
def flattener() -> Flattener:
    return Flattener()

@pytest.fixture
def flattened_json(flattener, submission_entities, schemas):
    return flattener.flatten(submission_entities, schemas)

@pytest.fixture
def expected(script_dir):
    return get_json_file(script_dir + '/small-project-flattened-with-schema.json')

def test_matches_expected(flattened_json, expected):
    assert_that(flattened_json).is_equal_to(expected, ignore='Schemas')
    assert_that(set(flattened_json.get('Schemas', []))).is_equal_to(set(expected.get('Schemas', [])))


def test_headers_exist_for_all_values(flattened_json):
    for sheet_name, sheet in flattened_json.items():
        if sheet_name != 'Schemas':
            columns = set()
            for row in sheet.get('values', []):
                if isinstance(row, dict):
                    columns.update(row.keys())
            for column in columns:
                assert column in flattened_json.get(sheet_name, {}).get('headers', {})
