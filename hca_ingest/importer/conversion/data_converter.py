from abc import abstractmethod
from enum import Enum

from hca_ingest.importer.conversion.exceptions import InvalidBooleanValue


class DataType(Enum):
    STRING = 'string'
    INTEGER = 'integer'
    BOOLEAN = 'boolean'
    NUMBER = 'number'
    UNDEFINED = 'undefined'

    @staticmethod
    def find(value: str):
        data_type = DataType.UNDEFINED
        if value is not None:
            try:
                data_type = DataType(value.lower())
            except ValueError:
                pass
        return data_type


class Converter:

    @abstractmethod
    def convert(self, data):
        raise NotImplementedError()

    @abstractmethod
    def type(self):
        raise NotImplementedError()


class DefaultConverter(Converter):

    def convert(self, data):
        return data

    def type(self):
        return DataType.UNDEFINED


class StringConverter(Converter):

    def convert(self, data):
        return str(data).strip()

    def type(self):
        return DataType.STRING


class IntegerConverter(Converter):

    def convert(self, data):
        try:
            return int(data)
        except ValueError:
            return int(float(data))

    def type(self):
        return DataType.INTEGER

class NumberConverter(Converter):

    def convert(self, data):
        return float(data)

    def type(self):
        return DataType.NUMBER


BOOLEAN_TABLE = {
    'true': True,
    'yes': True,
    'false': False,
    'no': False
}


class BooleanConverter(Converter):

    def convert(self, data):
        value = BOOLEAN_TABLE.get(data.lower())
        if value is None:
            raise InvalidBooleanValue(data)
        return value

    def type(self):
        return DataType.BOOLEAN


CONVERTER_MAP = {
    DataType.STRING: StringConverter(),
    DataType.INTEGER: IntegerConverter(),
    DataType.BOOLEAN: BooleanConverter(),
    DataType.NUMBER: NumberConverter()

}


class ListConverter(Converter):

    def __init__(self, data_type: DataType = DataType.STRING, base_converter: Converter = None):
        self.base_type = data_type
        if base_converter is not None:
            self.base_type = base_converter.type()
            self.base_converter = base_converter
        else:
            self.base_converter = CONVERTER_MAP.get(data_type, CONVERTER_MAP[DataType.STRING])

    def convert(self, data):
        data = str(data)
        value = data.split('||')
        value = [self.base_converter.convert(elem) for elem in value]
        return value

    def type(self):
        return self.base_type


DEFAULT = DefaultConverter()
