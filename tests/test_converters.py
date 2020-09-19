"""
    Unit test all converters
"""

import pytest
from marshmallow import Schema
from marshmallow import fields

from marshmallow_pyspark.converters import *
from marshmallow_pyspark.fields import Raw
from marshmallow_pyspark.schema import Schema


class MockConverter(ConverterABC):

    def convert(self, ma_field: fields.Field) -> DataType:
        return StringType()


def test_create():
    converter_map = {
        fields.String: MockConverter
    }
    converter = MockConverter(converter_map)

    assert converter.converter_map == converter_map


def test_convert():
    converter_map = {
        fields.String: MockConverter
    }
    converter = MockConverter(converter_map)
    assert converter.convert(fields.String()) == StringType()


@pytest.mark.parametrize("field_converter, ma_field, spark_type", [
    (RawConverter, Raw(spark_type=DateType()), DateType()),
    (StringConverter, fields.String(), StringType()),
    (DateTimeConverter, fields.DateTime(), TimestampType()),
    (DateConverter, fields.Date(), DateType()),
    (BooleanConverter, fields.Boolean(), BooleanType()),
    (IntegerConverter, fields.Integer(), IntegerType()),
    (FloatConverter, fields.Float(), FloatType()),
    (NumberConverter, fields.Number(), DoubleType()),
    (ListConverter, fields.List(fields.String()), ArrayType(StringType())),
    (DictConverter, fields.Dict(), MapType(StringType(), StringType())),
    (DictConverter, fields.Dict(fields.String(), fields.Number()), MapType(StringType(), DoubleType())),
    (
            NestedConverter,
            fields.Nested(Schema.from_dict({"name": fields.String()})),
            StructType([StructField("name", StringType())])
    ),
])
def test_field_converters(field_converter, ma_field, spark_type):
    assert field_converter(Schema.CONVERTER_MAP).convert(ma_field) == spark_type
