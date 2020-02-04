"""
    Unit tests for Schema
"""

import datetime
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
                {"name": "invalid_1", "age": "32.05", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_2", "age": "32", "expenses": "thirty", "employed": "False"},
                {"name": "invalid_3", "age": "32", "expenses": "30.5", "employed": "Fa"},
            ],
            [
                {"name": "valid_1", "age": 40, "expenses": 43.5, "employed": True},
                {"name": "valid_2", "age": 32, "expenses": 30.5, "employed": False},
            ],
            [
                {"name": "invalid_1", "age": "32.05", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_2", "age": "32", "expenses": "thirty", "employed": "False"},
                {"name": "invalid_3", "age": "32", "expenses": "30.5", "employed": "Fa"},
            ]
    ),
    (
            Schema.from_dict({
                "name": fields.String(required=True),
                "date": fields.Date(required=True),
                "date_time": fields.DateTime(required=True),
            }),
            [
                {"name": "valid_1", "date": "1970-10-15", "date_time": "1970-10-15 01:00:00"},
                {"name": "invalid_1", "date": "1970-10-15 00:00:00", "date_time": "1970-10-15"},
            ],
            [
                {"name": "valid_1",
                 "date": datetime.date(1970, 10, 15),
                 "date_time": datetime.datetime(1970, 10, 15, 1, 0)},
            ],
            [
                {"name": "invalid_1", "date": "1970-10-15 00:00:00", "date_time": "1970-10-15"},
            ]
    ),
    (
            Schema.from_dict({
                "name": fields.String(required=True),
                "book": fields.Nested(
                    Schema.from_dict({
                        "author": fields.String(required=True),
                        "title": fields.String(required=True),
                        "cost": fields.Number(required=True)
                    })
                )
            }),
            [
                {"name": "valid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": "32.5"}},
                {"name": "invalid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": "32a"}},
            ],
            [
                {"name": "valid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": 32.5}},
            ],
            [
                {"name": "invalid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": "32a"}},
            ]
    )
])
def test_load_df(spark_session, schema, input_data, valid_rows, invalid_rows):
    input_df = spark_session.createDataFrame(input_data)

    # Test with split
    valid_df, errors_df = schema().validate_df(input_df)
    _valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    assert _valid_rows == valid_rows
    error_rows = [json.loads(row[DEFAULT_ERRORS_COLUMN_NAME]) for row in errors_df.collect()]
    assert [row["row"] for row in error_rows] == invalid_rows


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
                {"name": "invalid_1", "age": "32.05", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_2", "age": "32", "expenses": "thirty", "employed": "False"},
                {"name": "invalid_3", "age": "32", "expenses": "30.5", "employed": "Fa"},
            ],
            [
                {"name": "valid_1", "age": 40, "expenses": 43.5, "employed": True},
                {"name": "valid_2", "age": 32, "expenses": 30.5, "employed": False},
            ],
            [
                {"name": "invalid_1", "age": "32.05", "expenses": "30.5", "employed": "False"},
                {"name": "invalid_2", "age": "32", "expenses": "thirty", "employed": "False"},
                {"name": "invalid_3", "age": "32", "expenses": "30.5", "employed": "Fa"},
            ]
    ),
    (
            Schema.from_dict({
                "name": fields.String(required=True),
                "date": fields.Date(required=True),
                "date_time": fields.DateTime(required=True),
            }),
            [
                {"name": "valid_1", "date": "1970-10-15", "date_time": "1970-10-15 01:00:00"},
                {"name": "invalid_1", "date": "1970-10-15 00:00:00", "date_time": "1970-10-15"},
            ],
            [
                {"name": "valid_1",
                 "date": datetime.date(1970, 10, 15),
                 "date_time": datetime.datetime(1970, 10, 15, 1, 0)},
            ],
            [
                {"name": "invalid_1", "date": "1970-10-15 00:00:00", "date_time": "1970-10-15"},
            ]
    ),
    (
            Schema.from_dict({
                "name": fields.String(required=True),
                "book": fields.Nested(
                    Schema.from_dict({
                        "author": fields.String(required=True),
                        "title": fields.String(required=True),
                        "cost": fields.Number(required=True)
                    })
                )
            }),
            [
                {"name": "valid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": "32.5"}},
                {"name": "invalid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": "32a"}},
            ],
            [
                {"name": "valid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": 32.5}},
            ],
            [
                {"name": "invalid_1", "book": {"author": "Sam", "title": "Sam's Book", "cost": "32a"}},
            ]
    )
])
def test_load_df_no_split(spark_session, schema, input_data, valid_rows, invalid_rows):
    input_df = spark_session.createDataFrame(input_data)

    # Test without split
    valid_df, errors_df = schema(split_errors=False).validate_df(input_df)
    assert errors_df is None
    _valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    for row in valid_rows:
        row[DEFAULT_ERRORS_COLUMN_NAME] = None
    assert all(row in _valid_rows for row in valid_rows)
