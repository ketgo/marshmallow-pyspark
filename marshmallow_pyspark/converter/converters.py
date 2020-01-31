"""
    Marshmallow fields to pyspark sql type converter
"""

from typing import Mapping, Type

from marshmallow import fields
from marshmallow.fields import Field
from pyspark.sql.types import *

from .base import ConverterABC


class StringConverter(ConverterABC):
    """
        String field converter
    """

    def convert(self, ma_field: Field) -> DataType:
        return StringType()


class DateTimeConverter(ConverterABC):
    """
        DateTime field converter
    """

    def convert(self, ma_field: Field) -> DataType:
        return TimestampType()


class DateConverter(ConverterABC):
    """
        Date field converter
    """

    def convert(self, ma_field: Field) -> DataType:
        return DateType()


class BooleanConverter(ConverterABC):
    """
        Boolean field converter
    """

    def convert(self, ma_field: Field) -> DataType:
        return BooleanType()


class IntegerConverter(ConverterABC):
    """
        Integer field converter
    """

    def convert(self, ma_field: Field) -> DataType:
        return IntegerType()


class NumberConverter(ConverterABC):
    """
        Number field converter
    """

    def convert(self, ma_field: Field) -> DataType:
        return FloatType()


class ListConverter(ConverterABC):
    """
        List field converter
    """

    def convert(self, ma_field: fields.List) -> DataType:
        inner_converter = self.converter_map.get(type(ma_field.inner), StringConverter)
        return ArrayType(inner_converter(self.converter_map).convert(ma_field.inner))


class DictConverter(ConverterABC):
    """
        Dict field converter
    """

    def convert(self, ma_field: fields.Dict) -> DataType:
        key_field_converter = self.converter_map.get(type(ma_field.key_field), StringConverter)
        value_field_converter = self.converter_map.get(type(ma_field.value_field), StringConverter)
        return MapType(
            key_field_converter(self.converter_map).convert(ma_field.key_field),
            value_field_converter(self.converter_map).convert(ma_field.value_field)
        )


class NestedConverter(ConverterABC):
    """
        Nested field converter
    """

    def convert(self, ma_field: fields.Nested) -> DataType:
        _fields = []
        for field_name, nested_field in ma_field.schema._declared_fields.items():
            field_converter = self.converter_map.get(type(nested_field), StringConverter)
            _fields.append(
                StructField(field_name, field_converter(self.converter_map).convert(nested_field), nullable=True)
            )
        return StructType(_fields)


class Converter:
    """
        Marshmallow fields to pyspark sql type converter
    """
    # Map of marshmallow field types and corresponding converters
    CONVERTER_MAP: Mapping[Type[Field], Type[ConverterABC]] = {
        fields.String: StringConverter,
        fields.DateTime: DateTimeConverter,
        fields.Date: DateConverter,
        fields.Boolean: BooleanConverter,
        fields.Integer: IntegerConverter,
        fields.Number: NumberConverter,
        fields.List: ListConverter,
        fields.Dict: DictConverter,
        fields.Nested: NestedConverter,
    }

    def convert(self, ma_field: Field) -> DataType:
        converter = self.CONVERTER_MAP.get(type(ma_field), StringConverter)
        return converter(self.CONVERTER_MAP).convert(ma_field)
