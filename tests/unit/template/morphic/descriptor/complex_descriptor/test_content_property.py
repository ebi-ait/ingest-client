import unittest
import pytest

from hca_ingest.template.descriptor import ComplexPropertyDescriptor


@pytest.fixture
def metadata_schema_url():
    return "https://dev.schema.morphic.bio"


# TODO: Make use of fixture to avoid code duplication
class TestComplexPropertyDescriptor:
    """ Testing class for the ComplexPropertyDescriptor of the Descriptor class with 'content' property. """

    def test__description_with_content_property__succeeds(self, metadata_schema_url):
        top_level_metadata_schema_url = metadata_schema_url + "/type/project/1.0.0/study"
        sample_complex_metadata_schema_json = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": metadata_schema_url + "/type/project/1.0.0/study",
            "description": "A study entity defines the fields needed for representing a study.",
            "required": [
                "content"
            ],
            "type": "object",
            "properties": {
                "content": {
                    "description": "A study entity defines the fields needed for representing a study.",
                    "required": [
                        "contact_first_name",
                        "contact_surname",
                    ],
                    "type": "object",
                    "user_friendly": "Content",
                    "properties": {
                        "contact_first_name": {
                            "description": "First name of the main person to contact for queries about this study.",
                            "user_friendly": "Contact first name",
                            "type": "string",
                            "minLength": 1,
                            "example": "Anu; Enrique; Galabina"
                        },
                        "contact_surname": {
                            "description": "Surname of the main person to contact for queries about this study.",
                            "user_friendly": "Contact surname",
                            "type": "string",
                            "minLength": 1,
                            "example": "Koci; Zucchi"
                        },
                        "cell_line_name": {
                            "type": "string",
                            "description": "Name of the cell lines used for this study.",
                            "user_friendly": "Cell line name",
                            "enum": [
                                "KOLF2.2J"
                            ],
                            "example": "KOLF2.2J"
                        }
                    }
                }
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        expected_top_level_schema_descriptor = {
            "high_level_entity": "type", "domain_entity": "project", "module": "study", "version": "1.0.0",
            "url": top_level_metadata_schema_url
        }
        expected_top_level_schema_properties = {
            "description": "A study entity defines the fields needed for representing a study.",
            "value_type": "object", "multivalue": False, "external_reference": False, "required": False,
            "identifiable": False
        }
        expected_child_property_contact_first_name = {
            "description": "First name of the main person to contact for queries about this study.", "value_type":
                "string", "example": "Anu; Enrique; Galabina", "multivalue": False, "external_reference": False,
            "user_friendly": "Contact first name", "required": True, "identifiable": False
        }

        expected_child_property_contact_surname = {
            "description": "Surname of the main person to contact for queries about this study.",
            "value_type": "string", "example": "Koci; Zucchi", "multivalue": False, "external_reference": False,
            "user_friendly": "Contact surname", "required": True, "identifiable": False
        }
        expected_child_property_cell_line_name = {
            "description": "Name of the cell lines used for this study.",
            "value_type": "string", "example": "KOLF2.2J", "multivalue": False, "external_reference": False,
            "user_friendly": "Cell line name", "required": False, "identifiable": False
        }

        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor,
            "contact_first_name": expected_child_property_contact_first_name,
            "contact_surname": expected_child_property_contact_surname,
            "cell_line_name": expected_child_property_cell_line_name,
            "required_properties": ["contact_first_name", "contact_surname"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        assert descriptor.get_dictionary_representation_of_descriptor() == expected_dictionary_representation

    def test__description_with_content_property_and_multivalue_entity__succeeds(self, metadata_schema_url):
        top_level_metadata_schema_url = metadata_schema_url + "/type/project/1.0.0/study"
        sample_complex_metadata_schema_json = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": metadata_schema_url + "/type/project/1.0.0/study",
            "description": "A study entity defines the fields needed for representing a study.",
            "required": [
                "content"
            ],
            "type": "object",
            "properties": {
                "content": {
                    "description": "A study entity defines the fields needed for representing a study.",
                    "required": [
                        "contact_first_name",
                        "perturbation_type",
                    ],
                    "type": "object",
                    "user_friendly": "Content",
                    "properties": {
                        "contact_first_name": {
                            "description": "First name of the main person to contact for queries about this study.",
                            "user_friendly": "Contact first name",
                            "type": "string",
                            "minLength": 1,
                            "example": "Anu; Enrique; Galabina"
                        },
                        "perturbation_type": {
                            "description": "Type of perturbation introduced by the gene expression alteration assay.",
                            "user_friendly": "Perturbation type(s)",
                            "type": "array",
                            "items": {
                                "type": "string"
                            },
                            "example": "irreversible gene knockout; reversible gene knockdown"
                        }
                    }
                }
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        expected_top_level_schema_descriptor = {
            "high_level_entity": "type", "domain_entity": "project", "module": "study", "version": "1.0.0",
            "url": top_level_metadata_schema_url
        }
        expected_top_level_schema_properties = {
            "description": "A study entity defines the fields needed for representing a study.",
            "value_type": "object", "multivalue": False, "external_reference": False, "required": False,
            "identifiable": False
        }
        expected_child_property_contact_first_name = {
            "description": "First name of the main person to contact for queries about this study.", "value_type":
                "string", "example": "Anu; Enrique; Galabina", "multivalue": False, "external_reference": False,
            "user_friendly": "Contact first name", "required": True, "identifiable": False
        }
        expected_child_property_perturbation_type = {
            "description": "Type of perturbation introduced by the gene expression alteration assay.",
            "value_type": "string", "example": "irreversible gene knockout; reversible gene knockdown",
            "multivalue": True, "external_reference": False, "user_friendly": "Perturbation type(s)", "required": True,
            "identifiable": False
        }

        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor,
            "contact_first_name": expected_child_property_contact_first_name,
            "perturbation_type": expected_child_property_perturbation_type,
            "required_properties": ["contact_first_name", "perturbation_type"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        assert descriptor.get_dictionary_representation_of_descriptor() == expected_dictionary_representation
