import unittest
import pytest

from hca_ingest.template.descriptor import ComplexPropertyDescriptor


@pytest.fixture
def metadata_schema_url():
    return "https://dev.schema.morphic.bio"


# TODO: Make use of fixture to avoid code duplication
class TestComplexPropertyDescriptor:
    """ Testing class for the ComplexPropertyDescriptor of the Descriptor class with 'content' property containing
    ontology entity."""

    def test__description_with_content_property_and_ontology_entity__succeeds(self, metadata_schema_url):
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

        # Create the top level expected dictionary representation
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

        # Create the expected dictionary representation of the ontology property.
        expected_ontology_properties = {
            "description": "High throughput readout assay used in the generation of the study.", "value_type": "string",
            "example": "EFO:0008931; EFO:0009310", "multivalue": False, "external_reference": False,
            "user_friendly": "Readout assay", "required": True, "identifiable": False
        }

        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor,
            "contact_first_name": expected_child_property_contact_first_name,
            "readout_assay": expected_ontology_properties,
            "required_properties": ["contact_first_name", "readout_assay"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        assert descriptor.get_dictionary_representation_of_descriptor() == expected_dictionary_representation

    def test__description_with_content_property_and_array_ontology_entity__succeeds(self, metadata_schema_url):
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
                        "sequencing_platforms"
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
                        "sequencing_platforms": {
                            "description": "Sequencing platform(s) being used to sequence the samples.",
                            "user_friendly": "Sequencing platform(s)",
                            "type": "array",
                            "items": {
                                "description": "Sequencing platform being used to sequence the samples",
                                "user_friendly": "Sequencing platform",
                                "type": "string",
                                "graphRestriction": {
                                    "ontologies": [
                                        "obo:efo"
                                    ],
                                    "classes": [
                                        "OBI:0000070"
                                    ],
                                    "includeSelf": False,
                                    "queryFields": [
                                        "obo_id",
                                        "label"
                                    ]
                                },
                                "example": "EFO:0008440; EFO:0008441"
                            }
                        }
                    }
                }
            }
        }

        descriptor = ComplexPropertyDescriptor(sample_complex_metadata_schema_json)

        # Create the top level expected dictionary representation
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

        # Create the expected dictionary representation of the ontology property.
        expected_ontology_properties = {
            "description": "Sequencing platform being used to sequence the samples", "value_type": "string",
            "example": "EFO:0008440; EFO:0008441", "multivalue": True, "external_reference": False,
            "user_friendly": "Sequencing platform", "required": True, "identifiable": False
        }

        expected_dictionary_representation = {
            "schema": expected_top_level_schema_descriptor,
            "contact_first_name": expected_child_property_contact_first_name,
            "sequencing_platforms": expected_ontology_properties,
            "required_properties": ["contact_first_name", "sequencing_platforms"]
        }
        expected_dictionary_representation.update(expected_top_level_schema_properties)
        assert descriptor.get_dictionary_representation_of_descriptor() == expected_dictionary_representation
