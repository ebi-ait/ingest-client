import unittest
import pytest

from hca_ingest.template.descriptor import ComplexPropertyDescriptor


@pytest.fixture
def metadata_schema_url():
    return "https://d1wew2wvfg0okw.cloudfront.net"


@pytest.fixture
def sample_complex_metadata_schema_json(metadata_schema_url):
    return {
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


# TODO: Make use of fixture to avoid code duplication and split cases into individual tests
class TestComplexPropertyDescriptor:
    """ Testing class for the ComplexPropertyDescriptor of the Descriptor class. """

    def test__description_with_content_property__succeeds(self, metadata_schema_url,
                                                          sample_complex_metadata_schema_json):
        top_level_metadata_schema_url = metadata_schema_url + "/type/project/1.0.0/study"
        print(sample_complex_metadata_schema_json)
        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        expected_child_property_contact_first_name, expected_top_level_schema_descriptor, \
            expected_top_level_schema_properties = self.top_level_schema_properties(top_level_metadata_schema_url)

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

    # TODO: Make sure whether ontology items will need to be handled separately
    def test__description_with_content_property_and_ontology_entity__succeeds(self, metadata_schema_url,
                                                                              sample_complex_metadata_schema_json):
        top_level_metadata_schema_url = metadata_schema_url + "/type/project/1.0.0/study"
        sample_complex_metadata_schema_json = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": top_level_metadata_schema_url,
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
                        "readout_assay"
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
                        "readout_assay": {
                            "description": "High throughput readout assay used in the generation of the study.",
                            "user_friendly": "Readout assay",
                            "type": "string",
                            "graphRestriction": {
                                "ontologies": [
                                    "obo:efo"
                                ],
                                "classes": [
                                    "EFO:0002772"
                                ],
                                "includeSelf": False,
                                "queryFields": [
                                    "obo_id",
                                    "label"
                                ]
                            },
                            "example": "EFO:0008931; EFO:0009310"
                        }
                    }
                }
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        # Create the expected dictionary representation of the ontology property.
        expected_ontology_descriptor = {
            "high_level_entity": "type", "domain_entity": "ontology", "module": "readout_assay",
            "version": "", "url": ""
        }
        expected_ontology_properties = {
            "description": "High throughput readout assay used in the generation of the study.", "value_type": "string",
            "example": "EFO:0008931; EFO:0009310", "multivalue": False, "external_reference": False,
            "user_friendly": "Readout assay", "required": True, "identifiable": False
        }
        expected_child_property_ontology_descriptor = {
            "description": "High throughput readout assay used in the generation of the study.", "value_type": "string",
            "example": "EFO:0008931; EFO:0009310", "multivalue": False, "external_reference": False,
            "user_friendly": "Readout assay", "required": False, "identifiable": False
        }
        expected_child_property_readout_assay_descriptor = {
            "schema": expected_ontology_descriptor, "ontology": expected_child_property_ontology_descriptor
        }
        expected_child_property_readout_assay_descriptor.update(expected_ontology_properties)

        # Create the top level expected dictionary representation
        expected_child_property_contact_first_name, expected_top_level_schema_descriptor, \
            expected_top_level_schema_properties = self.top_level_schema_properties(top_level_metadata_schema_url)

        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor,
            "contact_first_name": expected_child_property_contact_first_name,
            "readout_assay": expected_child_property_readout_assay_descriptor,
            "required_properties": ["contact_first_name", "readout_assay"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        assert descriptor.get_dictionary_representation_of_descriptor() == expected_dictionary_representation

    def top_level_schema_properties(self, top_level_metadata_schema_url):
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
        return expected_child_property_contact_first_name, expected_top_level_schema_descriptor, \
            expected_top_level_schema_properties
