import unittest
from unittest.mock import patch, Mock

from hca_ingest.template.schema_parser import SchemaParser
from hca_ingest.template.schema_template import SchemaTemplate


class TestSchemaTemplate(unittest.TestCase):
    """ Testing class for the SchemaTemplate class. """

    @patch("requests.get")
    def test__creation_of_template_with_json_with_content_property__success(self, property_migrations_request_mock):
        property_migrations_request_mock.return_value = Mock(ok=True)
        property_migrations_request_mock.return_value.json.return_value = {'migrations': []}

        sample_metadata_schema_json = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://d1wew2wvfg0okw.cloudfront.net/type/project/1.0.0/study",
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
                        "contact_surname"
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
                        }
                    }
                }
            }
        }

        schema_template = SchemaTemplate(json_schema_docs=[sample_metadata_schema_json])

        expected_schema_version = "1.0.0"
        expected_schema_metadata_properties = {
            "study": SchemaParser(sample_metadata_schema_json).schema_dictionary}
        expected_schema_labels = {"study.contact_first_name": ["study.contact_first_name"],
                                  "contact first name": ["study.contact_first_name"],
                                  "study.contact_surname": ["study.contact_surname"],
                                  "contact surname": ["study.contact_surname"]}
        expected_schema_tabs = [{"study": {"display_name": "Study",
                                           "columns": ["study.contact_first_name",
                                                       "study.contact_surname"]}}]

        self.assertEqual(schema_template.template_version, expected_schema_version)
        self.assertEqual(schema_template.meta_data_properties, expected_schema_metadata_properties)
        self.assertEqual(schema_template.labels, expected_schema_labels)
        self.assertEqual(schema_template.tabs, expected_schema_tabs)
