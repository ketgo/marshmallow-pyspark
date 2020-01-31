"""
    Unit test converter base
"""

from marshmallow.fields import Field, String
from pyspark.sql.types import DataType, StringType

from marshmallow_pyspark.converter.base import ConverterABC


class MockConverter(ConverterABC):

    def convert(self, ma_field: Field) -> DataType:
        return StringType()


def test_create():
    converter_map = {
        String: MockConverter
    }
    converter = MockConverter(converter_map)

    assert converter.converter_map == converter_map


def test_convert():
    converter_map = {
        String: MockConverter
    }
    converter = MockConverter(converter_map)
    assert converter.convert(String()) == StringType()
