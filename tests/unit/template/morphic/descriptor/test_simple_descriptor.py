import unittest

from hca_ingest.template.descriptor import SimplePropertyDescriptor


class TestSimplePropertyDescriptor(unittest.TestCase):
    """ Testing class for the SimplePropertyDescriptor of the Descriptor class. """

    def test__descriptor_with_user_friendly_field__succeeds(self):
        sample_simple_property_description = "The version number of the schema in major.minor.patch format."
        sample_simple_property_type = "string"
        sample_simple_property_pattern = "^[0-9]{1,}.[0-9]{1,}.[0-9]{1,}$"
        sample_simple_property_example = "4.6.1"
        sample_simple_property_user_friendly = "Project description"
        sample_simple_metadata_schema_json = {

            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "pattern": sample_simple_property_pattern,
            "example": sample_simple_property_example,
            "user_friendly": sample_simple_property_user_friendly
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_type,
            "example": sample_simple_property_example, "user_friendly": sample_simple_property_user_friendly,
            "multivalue": False, "external_reference": False, "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__descriptor_with_title_instead_of_user_friendly_field__succeeds(self):
        sample_simple_property_description = "The version number of the schema in major.minor.patch format."
        sample_simple_property_type = "string"
        sample_simple_property_pattern = "^[0-9]{1,}.[0-9]{1,}.[0-9]{1,}$"
        sample_simple_property_example = "4.6.1"
        sample_simple_property_user_friendly = "Project description"
        sample_simple_metadata_schema_json = {

            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "pattern": sample_simple_property_pattern,
            "example": sample_simple_property_example,
            "title": sample_simple_property_user_friendly
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_type,
            "example": sample_simple_property_example, "user_friendly": sample_simple_property_user_friendly,
            "multivalue": False, "external_reference": False, "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__descriptor_with_examples_field_of_strings__succeeds(self):
        sample_simple_property_description = "The version number of the schema in major.minor.patch format."
        sample_simple_property_type = "string"
        sample_simple_property_pattern = "^[0-9]{1,}.[0-9]{1,}.[0-9]{1,}$"
        sample_simple_property_example = "4.6.1, 2.2.1"
        sample_simple_metadata_schema_json = {

            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "pattern": sample_simple_property_pattern,
            "examples": sample_simple_property_example
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_type,
            "example": sample_simple_property_example, "multivalue": False, "external_reference": False,
            "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__descriptor_with_examples_field_of_array_of_strings__succeeds(self):
        sample_simple_property_description = "High throughput readout assay used in the generation of the study."
        sample_simple_property_type = "string"
        sample_simple_property_example = [
            "EFO:0008931",
            "EFO:0009310"
        ]
        sample_simple_metadata_schema_json = {

            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "examples": sample_simple_property_example
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_type,
            "example": "EFO:0008931; EFO:0009310", "multivalue": False, "external_reference": False,
            "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)

    def test__descriptor_with_examples_field_of_array_of_integers__succeeds(self):
        sample_simple_property_description = "Value of the library concentration."
        sample_simple_property_type = "number"
        sample_simple_property_example = [
            7.5,
            8,
            9.1
        ]
        sample_simple_metadata_schema_json = {

            "description": sample_simple_property_description,
            "type": sample_simple_property_type,
            "examples": sample_simple_property_example
        }

        descriptor = SimplePropertyDescriptor(sample_simple_metadata_schema_json)

        expected_dictionary_representation = {
            "description": sample_simple_property_description, "value_type": sample_simple_property_type,
            "example": "7.5; 8; 9.1", "multivalue": False, "external_reference": False,
            "required": False, "identifiable": False
        }
        self.assertEqual(descriptor.get_dictionary_representation_of_descriptor(), expected_dictionary_representation)
