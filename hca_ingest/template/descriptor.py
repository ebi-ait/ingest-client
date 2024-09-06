#!/usr/bin/env python
"""
Class encapsulating implementation details on the Descriptor classes. Descriptors represent a portion of a metadata
schema.
"""
import os
import re

IDENTIFIABLE_PROPERTIES = ["biomaterial_id", "process_id", "protocol_id", "file_name"]


class Descriptor():
    """ Parent class type. A Descriptor type encapsulate a small isolated amount of information about a portion of a
    metadata schema.
    """

    def get_dictionary_representation_of_descriptor(self):
        """ Returns a dict representing the Descriptor object. """
        raise NotImplementedError("Subclasses of Descriptor are required to override this method.")


class SchemaTypeDescriptor(Descriptor):
    """ Descriptor encapsulating "metadata" information about a single metadata schema file. """

    def __init__(self, metadata_schema_url, json_data=None):
        url_parts = metadata_schema_url.split('/')

        self.high_level_entity = None
        self.version = None
        self.domain_entity = None
        self.module = None
        self.url = metadata_schema_url
        self.accession = None  # New attribute for ENA
        self.title = None  # New attribute for ENA

        high_level_entity_found = False
        version_found = False

        project_env = os.getenv('PROJECT_ENV', 'default').upper()

        if project_env == 'ENA':
            self.parse_ena_url(metadata_schema_url, json_data)
        else:
            for i, part in enumerate(url_parts):
                if part in ['type', 'module', 'core', 'system']:
                    self.high_level_entity = part
                    high_level_entity_found = True
                elif (part.replace('.', '').isdigit() or part == 'latest') and high_level_entity_found:
                    self.version = part
                    version_found = True
                    # If the version comes right after the high_level_entity
                    if i == url_parts.index(self.high_level_entity) + 1:
                        # Domain entity is everything between version and the last part (module)
                        if i < len(url_parts) - 2:
                            self.domain_entity = '/'.join(url_parts[i + 1:-1])
                        self.module = url_parts[-1]
                    else:
                        # If the version comes after domain_entity
                        self.domain_entity = '/'.join(url_parts[url_parts.index(self.high_level_entity) + 1:i])
                        self.module = url_parts[-1]
                    break
                elif high_level_entity_found and not version_found:
                    # Handle case where the version comes after the domain_entity
                    self.domain_entity = part

        if not self.high_level_entity or not self.version or not self.module:
            raise Exception(f"ERROR: The metadata schema URL {metadata_schema_url} does not conform to expected format.")

    def parse_ena_url(self, metadata_schema_url, json_data):
        """Parsing logic specific for ENA URLs using regex."""

        pattern = re.compile(
            r'^http[s]?://(?P<location>[^/]+)/' +
            r'(?P<high_level_entity>[^/]+)/' +
            r'(?P<domain_entity>schemas)/' +
            r'(?P<module>[^/]+)/' +
            r'(?P<version>[^/]+)$'
        )

        match = pattern.match(metadata_schema_url)

        if match:
            self.high_level_entity = match.group('high_level_entity')
            self.domain_entity = match.group('domain_entity')
            self.module = match.group('module')
            self.version = match.group('version')

            if json_data:
                self.accession = json_data.get("accession", None)
                self.title = json_data.get("title", None)
        else:
            raise Exception(f"ERROR: The ENA metadata schema URL {metadata_schema_url} does not conform to expected "
                            f"format.")

    def get_accession(self):
        return self.accession

    def get_title(self):
        return self.title

    def get_module(self):
        return self.module

    def get_dictionary_representation_of_descriptor(self):
        """ Returns a dictionary representation of the current schema descriptor object. """
        return self.__dict__


class SimplePropertyDescriptor(Descriptor):
    """ A Descriptor encapsulating information about a simple property of a metadata schema. A simple property is
    designated as having no children properties which arises when the property is associated with its own metadata
    schema.
    """

    def __init__(self, json_data):
        """ Initialize the simply property descriptor using the top level fields in given json data. """
        self.user_friendly = None
        self.example = None
        self.value_type = json_data.get("type")
        self.multivalue = False

        if self.value_type == "array":
            self.multivalue = True
            # Get the type of elements in the array which is nested inside the "items" key.
            # self.value_type = json_data["items"]["type"]
            self.value_type = self.get_nested_type(json_data.get("items", {}))

        self.format = json_data.get("format")
        self.get_user_friendly(json_data)
        self.description = json_data.get("description")
        self.get_example(json_data)
        self.guidelines = json_data.get("guidelines")

        # For now, required, external_reference and identifiable are set to false because the value of these properties
        # exist in the parent metadata schema and not in the property description itself. They will be back-populated
        # later.
        self.required = False
        self.identifiable = False
        self.external_reference = False

    def get_nested_type(self, items):
        """ Recursively retrieve the most nested type within the items structure.
            Default to 'string' if 'text' is present and no type is specified.
        """
        if "type" in items:
            if items["type"] == "object" and "properties" in items:
                # If it's an object, continue digging into the properties to find the most specific type
                for key, value in items["properties"].items():
                    # Recursively search the nested structure
                    nested_type = self.get_nested_type(value)
                    if nested_type:
                        return nested_type
            else:
                return items["type"]
        elif "enum" in items and "text" in items:
            # If 'text' is a key and no type is explicitly defined, default to 'string'
            return "string"
        return None

    def get_user_friendly(self, json_data):
        if json_data.get("user_friendly"):
            self.user_friendly = json_data.get("user_friendly")
        elif not self.is_schema_object(json_data) and json_data.get("title"):
            self.user_friendly = json_data.get("title")
        else:
            self.user_friendly = None

    def get_example(self, json_data):
        if json_data.get("example"):
            self.example = json_data.get("example")
        elif json_data.get("examples"):
            if isinstance(json_data.get("examples"), list):
                self.example = '; '.join(map(str, json_data.get("examples")))
            elif isinstance(json_data.get("examples"), str):
                self.example = json_data.get("examples")
        else:
            self.example = None

    def is_schema_object(self, json_data):
        return json_data.get("$schema") or json_data.get("schema")

    def get_dictionary_representation_of_descriptor(self):
        """ Only include information in the class where the value is not None or empty OR if the value is a boolean
        since in that case, both True and False are valid values."""

        return dict((key, value) for (key, value) in self.__dict__.items() if value or isinstance(value, bool))


def check_if_property_identifiable(child_property_descriptor, property_name):
    if property_name in IDENTIFIABLE_PROPERTIES:
        child_property_descriptor.identifiable = True


class ComplexPropertyDescriptor(SimplePropertyDescriptor, Descriptor):
    """ A Descriptor encapsulating information about a complex property of a metadata schema. A complex property
    means that there exists an entire metadata schema to describe the property itself and usually contains children
    properties."""

    def __init__(self, json_data):
        super().__init__(json_data)

        # Initialize schema object
        self.schema = None

        # Initialize required fields
        self.required_properties = None

        # Initialize children properties
        self.children_properties = {}

        # Populate metadata/information about the schema itself, derived from the URL
        self.populate_schema_information(json_data)

        # Add children properties
        self.add_children_properties(json_data)

    def add_children_properties(self, json_data):

        # Add required fields
        self.required_properties = json_data.get("required")

        properties = json_data.get("properties")
        if not properties and "schema" in json_data:
            properties = json_data["schema"].get("properties")

        if properties:
            for property_name, property_values in properties.items():
                if self.is_schema_field(property_values):
                    child_property_descriptor = ComplexPropertyDescriptor(property_values)
                elif self.is_content_field(property_name, property_values):
                    self.add_children_properties(property_values)
                    # 'content' property name should not be included in the children properties so continue
                    continue
                elif self.is_array_field(property_values) and (
                        self.is_schema_field(property_values["items"])):
                    child_property_descriptor = ComplexPropertyDescriptor(property_values["items"])

                    child_property_descriptor.multivalue = True
                    self.assign_items_user_friendly(child_property_descriptor, property_values)
                elif self.is_array_field(property_values) and self.is_object_field(property_values["items"]):
                    # Handle inline embedded schema (like "gene_edition")
                    child_property_descriptor = ComplexPropertyDescriptor(property_values["items"])
                    child_property_descriptor.multivalue = True

                    self.assign_items_user_friendly(child_property_descriptor, property_values)

                    # ENA: Get "description" for nested array object. If it has a "text" field, set the type to "string"
                    child_property_descriptor.description = property_values.get("description", "")

                    if "text" in property_values["items"].get("properties", {}):
                        child_property_descriptor.value_type = "string"
                elif self.is_array_ontology_field(property_values):
                    child_property_descriptor = ComplexPropertyDescriptor(property_values["items"])
                    child_property_descriptor.multivalue = True
                elif self.is_object_field(property_values):
                    # Handles standalone objects with nested properties (e.g., "distribution")
                    child_property_descriptor = ComplexPropertyDescriptor(property_values)
                else:
                    child_property_descriptor = SimplePropertyDescriptor(property_values)

                # Make it required if the child property name is in the list of required properties
                self.check_if_property_required(child_property_descriptor, property_name)

                # Make the property identifiable if the child property name is one of the listed hardcoded
                # identifiable properties
                check_if_property_identifiable(child_property_descriptor, property_name)

                self.children_properties[property_name] = child_property_descriptor

    def assign_items_user_friendly(self, child_property_descriptor, property_values):
        if child_property_descriptor.user_friendly is None:
            if "user_friendly" in property_values:
                child_property_descriptor.user_friendly = property_values["user_friendly"]
            elif "title" in property_values:
                child_property_descriptor.user_friendly = property_values["title"]

    def is_array_ontology_field(self, property_values):
        return self.is_array_field(property_values) and "graphRestriction" in property_values["items"]

    def check_if_property_required(self, child_property_descriptor, property_name):
        if self.required_properties and property_name in self.required_properties:
            child_property_descriptor.required = True

    def is_array_field(self, property_values):
        return "items" in property_values

    def is_schema_field(self, property_values):
        return "$schema" in property_values or "schema" in property_values

    def is_content_field(self, property_name, property_values):
        return "content" == property_name and "properties" in property_values

    def is_object_field(self, property_values):
        return "type" in property_values and property_values["type"] == "object"

    def populate_schema_information(self, json_data):
        if "$id" in json_data.keys():
            self.schema = SchemaTypeDescriptor(json_data["$id"])
        elif "id" in json_data.keys():
            self.schema = SchemaTypeDescriptor(json_data["id"])
        else:
            self.schema = None

    def get_schema_module_name(self):
        return self.schema.get_module()

    def get_dictionary_representation_of_descriptor(self):
        """ Returns a representation of the class as a dictionary with the following caveats:
        1) If the value of a key is None or empty but is NOT a boolean, then the attribute it omitted from the
        dictionary.
        2) If the value is of a SchemaTypeDescriptor type, convert it to a dictionary.
        3) Any child descriptors are flattened from being a list to simply added attributes where the key is the
        metadata schema name and the dictionary is the corresponding descriptor.
        """

        dictionary_representation = {}

        for (key, value) in self.__dict__.items():
            if key == "children_properties":
                for child_key, child_value in value.items():
                    self.add_key_value_to_dictionary_if_valid(child_key, child_value, dictionary_representation)
            else:
                self.add_key_value_to_dictionary_if_valid(key, value, dictionary_representation)

        return dictionary_representation

    @staticmethod
    def add_key_value_to_dictionary_if_valid(key, value, dictionary):
        if not value and not isinstance(value, bool):
            return
        if issubclass(type(value), Descriptor):
            dictionary[key] = value.get_dictionary_representation_of_descriptor()
        else:
            dictionary[key] = value
