import pytest
from assertpy import assert_that

from downloader.entity import Entity
from downloader.schema_collector import SchemaCollector

def get_entities_from_content_list(content_list):
    entity_list = []
    for index, content in enumerate(content_list):
        entity_list.append({
            'content': content,
            'uuid': {
                'uuid': f'uuid{index+1}'
            }
        })
    return Entity.from_json_list(entity_list)


@pytest.fixture
def multiple_schema_versions_of_same_concrete_entity():
    return get_entities_from_content_list([
        {
            "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/donor_organism",
            "schema_type": "biomaterial",
            "field": "value"
        },{
            "describedBy": "https://schema.humancellatlas.org/type/project/14.3.0/donor_organism",
            "schema_type": "biomaterial",
            "field": "value"
        }
    ])


@pytest.fixture
def schema_collector():
    return SchemaCollector()


def test_collection_raises_error(multiple_schema_versions_of_same_concrete_entity, schema_collector):
    with pytest.raises(ValueError) as value_error:
        schema_collector.get_schemas_for_entities(multiple_schema_versions_of_same_concrete_entity)
        assert_that(str(value_error.value)).is_equal_to("Multiple versions of same concrete entity schema")
