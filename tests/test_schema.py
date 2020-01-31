"""
    Unit tests for Schema
"""

import pytest
from marshmallow import fields
from pyspark.sql.types import *

from marshmallow_pyspark.constants import *
from marshmallow_pyspark.schema import Schema


def test_create():
    schema = Schema()
    assert schema.error_column_name == DEFAULT_ERRORS_COLUMN_NAME
    assert schema.split_invalid_rows == DEFAULT_SPLIT_INVALID_ROWS


@pytest.mark.parametrize("ma_field, spark_field", [
    (fields.String(), StringType()),
    (fields.DateTime(), TimestampType()),
    (fields.Date(), DateType()),
    (fields.Boolean(), BooleanType()),
    (fields.Integer(), IntegerType()),
    (fields.Number(), FloatType()),
    (fields.List(fields.String()), ArrayType(StringType())),
    (fields.Nested(Schema.from_dict({"name": fields.String()})), StructType([StructField("name", StringType())]))
])
def test_spark_schema(ma_field, spark_field):
    class TestSchema(Schema):
        test_column = ma_field

    spark_schema = StructType(
        [
            StructField("test_column", spark_field, nullable=True),
            StructField(DEFAULT_ERRORS_COLUMN_NAME, StringType(), nullable=True)
        ]
    )
    schema = TestSchema()
    assert schema.spark_schema == spark_schema

    spark_schema = StructType(
        [
            StructField("test_column", spark_field, nullable=True)
        ]
    )
    schema = TestSchema(error_column_name=False)
    assert schema.spark_schema == spark_schema
