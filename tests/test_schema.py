"""
    Unit tests for Schema
"""

import json

import pytest
from marshmallow import fields
from pyspark.sql.types import *

from marshmallow_pyspark.constants import *
from marshmallow_pyspark.schema import Schema


def test_create():
    schema = Schema()
    assert schema.error_column_name == DEFAULT_ERRORS_COLUMN_NAME
    assert schema.split_errors == DEFAULT_SPLIT_INVALID_ROWS
    assert schema.add_index_column == DEFAULT_ADD_INDEX_COLUMN


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


@pytest.mark.parametrize("schema, input_data, valid_rows, invalid_rows", [
    (
            Schema.from_dict({
                "name": fields.String(required=True),
                "age": fields.Integer(required=True),
                "expenses": fields.Float(required=True),
                "employed": fields.Boolean(required=True)
            }),
            [
                {"name": "valid_1", "age": "40", "expenses": "43.5", "employed": "True"},
                {"name": "valid_2", "age": "32", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_2", "age": "32.05", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_3", "age": "32", "expenses": "thirty", "employed": "False"},
                {"name": "invalid_4", "age": "32", "expenses": "30.5", "employed": "Fa"},
            ],
            [
                {"name": "valid_1", "age": 40, "expenses": 43.5, "employed": True},
                {"name": "valid_2", "age": 32, "expenses": 30.5, "employed": False},
            ],
            [
                {"name": "invalid_2", "age": "32.05", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_3", "age": "32", "expenses": "thirty", "employed": "False"},
                {"name": "invalid_4", "age": "32", "expenses": "30.5", "employed": "Fa"},
            ]
    ),
])
def test_load_df(spark_session, schema, input_data, valid_rows, invalid_rows):
    input_df = spark_session.createDataFrame(input_data)
    valid_df, errors_df = schema().validate_df(input_df)
    _valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    assert _valid_rows == valid_rows
    error_rows = [json.loads(row[DEFAULT_ERRORS_COLUMN_NAME]) for row in errors_df.collect()]
    assert [row["row"] for row in error_rows] == invalid_rows
