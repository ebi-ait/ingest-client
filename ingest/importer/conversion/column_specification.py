from enum import Enum

from ingest.importer.conversion import utils, data_converter
from ingest.importer.conversion.data_converter import DataType, CONVERTER_MAP, ListConverter


class ConversionType(Enum):
    UNDEFINED = 0,
    MEMBER_FIELD = 1,
    FIELD_OF_LIST_ELEMENT = 2,
    IDENTITY = 3,
    LINKED_IDENTITY = 4,
    EXTERNAL_REFERENCE = 5


class ColumnSpecification:

    def __init__(self, field_name, object_type, main_category, data_type, multivalue=False,
                 multivalue_parent=False, identity: bool=False, external_reference: bool=False):
        self.field_name = field_name
        self.object_type = object_type
        self.main_category = main_category
        self.data_type = data_type
        self.multivalue = multivalue
        self.multivalue_parent = multivalue_parent
        self.identity = identity
        self.external_reference = external_reference

    def is_multivalue(self):
        return self.multivalue

    def is_field_of_list_element(self):
        return self.multivalue_parent

    def is_identity(self):
        return self.identity

    def is_external_reference(self):
        return self.external_reference

    def get_conversion_type(self):
        if self.multivalue_parent:
            conversion_type = ConversionType.FIELD_OF_LIST_ELEMENT
        elif self.identity:
            entity_type = utils.extract_root_field(self.field_name)
            if entity_type == self.object_type:
                conversion_type = ConversionType.IDENTITY
            elif self.external_reference:
                conversion_type = ConversionType.EXTERNAL_REFERENCE
            else:
                conversion_type = ConversionType.LINKED_IDENTITY
        else:
            conversion_type = ConversionType.MEMBER_FIELD
        return conversion_type

    def determine_converter(self):
        if not self.multivalue:
            converter = CONVERTER_MAP.get(self.data_type, data_converter.DEFAULT)
        else:
            converter = ListConverter(self.data_type)
        return converter

    @staticmethod
    def build_raw(field_name, object_type, main_category, raw_spec, parent=None):
        data_type = DataType.find(raw_spec.get('value_type'))
        multivalue = bool(raw_spec.get('multivalue'))
        multivalue_parent = bool(parent.get('multivalue')) if parent != None else False
        identity: bool = bool(raw_spec.get('identifiable'))
        external_reference = bool(raw_spec.get('external_reference'))
        return ColumnSpecification(field_name, object_type, main_category, data_type,
                                   multivalue=multivalue, multivalue_parent=multivalue_parent,
                                   identity=identity, external_reference=external_reference)