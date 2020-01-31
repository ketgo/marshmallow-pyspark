"""
    Unit test all converters
"""

import pytest
from marshmallow import Schema

from marshmallow_pyspark.converter.converters import *


@pytest.mark.parametrize("field_converter, ma_field, spark_type", [
    (StringConverter, fields.String(), StringType()),
    (DateTimeConverter, fields.DateTime(), TimestampType()),
    (DateConverter, fields.Date(), DateType()),
    (BooleanConverter, fields.Boolean(), BooleanType()),
    (IntegerConverter, fields.Integer(), IntegerType()),
    (NumberConverter, fields.Number(), FloatType()),
    (ListConverter, fields.List(fields.String()), ArrayType(StringType())),
    (DictConverter, fields.Dict(), MapType(StringType(), StringType())),
    (DictConverter, fields.Dict(fields.String(), fields.Number()), MapType(StringType(), FloatType())),
    (
            NestedConverter,
            fields.Nested(Schema.from_dict({"name": fields.String()})),
            StructType([StructField("name", StringType())])
    ),
])
def test_field_converters(field_converter, ma_field, spark_type):
    assert field_converter(Converter.CONVERTER_MAP).convert(ma_field) == spark_type


@pytest.mark.parametrize("ma_field, spark_type", [
    (fields.String(), StringType()),
    (fields.DateTime(), TimestampType()),
    (fields.Date(), DateType()),
    (fields.Boolean(), BooleanType()),
    (fields.Integer(), IntegerType()),
    (fields.Number(), FloatType()),
    (fields.List(fields.String()), ArrayType(StringType())),
    (fields.Dict(), MapType(StringType(), StringType())),
    (fields.Dict(fields.String(), fields.Number()), MapType(StringType(), FloatType())),
    (fields.Nested(Schema.from_dict({"name": fields.String()})), StructType([StructField("name", StringType())]))
])
def test_converter(ma_field, spark_type):
    assert Converter().convert(ma_field) == spark_type
