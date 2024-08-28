import unittest
import pytest

from hca_ingest.template.descriptor import SchemaTypeDescriptor


@pytest.fixture
def metadata_schema_url():
    return "https://dev.schema.morphic.bio"


class TestSchemaTypeDescriptor:
    """ Testing class for the SchemaTypeDescriptor of the Descriptor class. """

    def test__schema_type_descriptor__succeeds(self, metadata_schema_url):
        sample_metadata_schema_url = metadata_schema_url + "/type/1.0.0/biomaterial/library_preparation"

        descriptor = SchemaTypeDescriptor(sample_metadata_schema_url)

        expected_dictionary_representation = {"high_level_entity": "type", "domain_entity": "biomaterial",
                                              "module": "library_preparation", "version": "1.0.0",
                                              "url": sample_metadata_schema_url}
        assert descriptor.get_dictionary_representation_of_descriptor() == expected_dictionary_representation
