"""
    Marshmallow fields to pyspark sql type converter
"""

from abc import ABCMeta, abstractmethod
from typing import Mapping, Type

from marshmallow import fields as ma_fields
from pyspark.sql.types import (DataType, StringType, BooleanType,
                               TimestampType, DateType, IntegerType,
                               FloatType, DoubleType, ArrayType,
                               StructType, StructField, MapType)

from .fields import Raw


class ConverterABC(metaclass=ABCMeta):
    """
        Abstract base class for converter

        :param converter_map: mapping between marshmallow field and
            corresponding converter. Primarily used by nested composite
            fields like list, mapping, etc.
    """

    def __init__(self, converter_map: Mapping[Type[ma_fields.Field], Type["ConverterABC"]]):
        self._converter_map = converter_map

    @property
    def converter_map(self) -> Mapping[Type[ma_fields.Field], Type["ConverterABC"]]:
        """
            Map between marshmallow field and corresponding converter.
        """
        return self._converter_map

    @abstractmethod
    def convert(self, ma_field: ma_fields.Field) -> DataType:
        """
            Convert marshmallow field to spark data type.

            :param ma_field: marshmallow field instance
            :return: spark SQL data type instance
        """
        raise NotImplementedError()


class RawConverter(ConverterABC):
    """
        Raw field converter
    """

    def convert(self, field: Raw) -> DataType:
        return field.spark_type


class StringConverter(ConverterABC):
    """
        String field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return StringType()


class DateTimeConverter(ConverterABC):
    """
        DateTime field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return TimestampType()


class DateConverter(ConverterABC):
    """
        Date field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return DateType()


class BooleanConverter(ConverterABC):
    """
        Boolean field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return BooleanType()


class IntegerConverter(ConverterABC):
    """
        Integer field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return IntegerType()


class FloatConverter(ConverterABC):
    """
        Float field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return FloatType()


class NumberConverter(ConverterABC):
    """
        Number field converter
    """

    def convert(self, ma_field: ma_fields.Field) -> DataType:
        return DoubleType()


class ListConverter(ConverterABC):
    """
        List field converter
    """

    def convert(self, ma_field: ma_fields.List) -> DataType:
        inner_converter = self.converter_map.get(type(ma_field.inner), StringConverter)
        return ArrayType(inner_converter(self.converter_map).convert(ma_field.inner))


class DictConverter(ConverterABC):
    """
        Dict field converter
    """

    def convert(self, ma_field: ma_fields.Dict) -> DataType:
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

    def convert(self, ma_field: ma_fields.Nested) -> DataType:
        _fields = []
        for field_name, nested_field in ma_field.schema._declared_fields.items():  # pylint: disable=W0212
            field_converter = self.converter_map.get(type(nested_field), StringConverter)
            _fields.append(
                StructField(
                    field_name,
                    field_converter(self.converter_map).convert(nested_field),
                    nullable=True
                )
            )
        return StructType(_fields)
