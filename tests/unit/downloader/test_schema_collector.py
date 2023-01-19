import pytest
from assertpy import assert_that

from hca_ingest.downloader.schema_collector import SchemaCollector
from .conftest import get_entities_from_content_list


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
