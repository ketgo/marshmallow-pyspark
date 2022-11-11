"""
    Unit tests for Schema
"""

import datetime
import json

import pytest
from marshmallow import fields
from pyspark.sql.types import *
from pyspark.sql import Row

from marshmallow_pyspark.constants import *
from marshmallow_pyspark.schema import Schema, _RowValidator
from marshmallow_pyspark.fields import Raw


def test_create():
    schema = Schema()
    assert schema.error_column_name == DEFAULT_ERRORS_COLUMN
    assert schema.split_errors == DEFAULT_SPLIT_ERRORS


@pytest.mark.parametrize("ma_field, spark_field", [
    (fields.String(), StringType()),
    (fields.DateTime(), TimestampType()),
    (fields.Date(), DateType()),
    (fields.Boolean(), BooleanType()),
    (fields.Integer(), IntegerType()),
    (fields.Number(), DoubleType()),
    (fields.List(fields.String()), ArrayType(StringType())),
    (fields.Nested(Schema.from_dict({"name": fields.String()})), StructType([StructField("name", StringType())]))
])
def test_spark_schema(ma_field, spark_field):
    class TestSchema(Schema):
        test_column = ma_field

    spark_schema = StructType(
        [
            StructField("test_column", spark_field, nullable=True),
            StructField(DEFAULT_ERRORS_COLUMN, StringType(), nullable=True)
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
def test_validate_df(spark_session, schema, input_data, valid_rows, invalid_rows):
    input_df = spark_session.createDataFrame(input_data)

    # Test with split
    valid_df, errors_df = schema().validate_df(input_df)
    _valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    assert _valid_rows == valid_rows
    error_rows = [json.loads(row[DEFAULT_ERRORS_COLUMN]) for row in errors_df.collect()]
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
def test_validate_df_no_split(spark_session, schema, input_data, valid_rows, invalid_rows):
    input_df = spark_session.createDataFrame(input_data)

    # Test without split
    valid_df, errors_df = schema(split_errors=False).validate_df(input_df)
    assert errors_df is None
    _valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    for row in valid_rows:
        row[DEFAULT_ERRORS_COLUMN] = None
    assert all(row in _valid_rows for row in valid_rows)


def test_add_duplicate_counts(spark_session):
    # Single unique column test
    input_data = [
        {"title": "valid_1", "release_date": "2020-1-10"},
        {"title": "invalid_1", "release_date": "2020-1-11"},
        {"title": "invalid_1", "release_date": "2020-31-11"},
        {"title": "invalid_2", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = ["title"]

        title = fields.Str()
        release_date = fields.Date()

    df = TestSchema()._add_duplicate_counts(input_df)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    assert rows == [
        {'release_date': '2020-1-11', 'title': 'invalid_1', '__count__title': 1},
        {'release_date': '2020-31-11', 'title': 'invalid_1', '__count__title': 2},
        {'release_date': '2020-1-51', 'title': 'invalid_2', '__count__title': 1},
        {'release_date': '2020-1-10', 'title': 'valid_1', '__count__title': 1}
    ]

    # Compound unique column test
    input_data = [
        {"title": "valid_1", "release_date": "2020-1-10"},
        {"title": "invalid_1", "release_date": "2020-1-11"},
        {"title": "invalid_1", "release_date": "2020-31-11"},
        {"title": "invalid_2", "release_date": "2020-1-51"},
        {"title": "invalid_2", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = [["title", "release_date"]]

        title = fields.Str()
        release_date = fields.Date()

    df = TestSchema()._add_duplicate_counts(input_df)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    assert rows == [
        {'release_date': '2020-1-11', 'title': 'invalid_1', '__count__title~release_date': 1},
        {'release_date': '2020-31-11', 'title': 'invalid_1', '__count__title~release_date': 1},
        {'release_date': '2020-1-51', 'title': 'invalid_2', '__count__title~release_date': 1},
        {'release_date': '2020-1-51', 'title': 'invalid_2', '__count__title~release_date': 2},
        {'release_date': '2020-1-10', 'title': 'valid_1', '__count__title~release_date': 1}
    ]

    # Multiple unique columns test
    input_data = [
        {"title": "valid_1", "release_date": "2020-1-10"},
        {"title": "invalid_1", "release_date": "2020-1-11"},
        {"title": "invalid_1", "release_date": "2020-31-11"},
        {"title": "invalid_2", "release_date": "2020-1-51"},
        {"title": "invalid_2", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = ["title", "release_date"]

        title = fields.Str()
        release_date = fields.Date()

    df = TestSchema()._add_duplicate_counts(input_df)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    assert rows == [
        {'release_date': '2020-1-10', 'title': 'valid_1', '__count__title': 1, '__count__release_date': 1},
        {'release_date': '2020-1-11', 'title': 'invalid_1', '__count__title': 1, '__count__release_date': 1},
        {'release_date': '2020-1-51', 'title': 'invalid_2', '__count__title': 1, '__count__release_date': 1},
        {'release_date': '2020-1-51', 'title': 'invalid_2', '__count__title': 2, '__count__release_date': 2},
        {'release_date': '2020-31-11', 'title': 'invalid_1', '__count__title': 2, '__count__release_date': 1}
    ]


def test_validate_df_with_duplicates(spark_session):
    # Single unique column test
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10"},
        {"title": "title_2", "release_date": "2020-1-11"},
        {"title": "title_2", "release_date": "2020-3-11"},
        {"title": "title_3", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = ["title"]

        title = fields.Str()
        release_date = fields.Date()

    valid_df, errors_df = TestSchema().validate_df(input_df)
    valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    error_rows = [row.asDict(recursive=True) for row in errors_df.collect()]
    assert valid_rows == [
        {'title': 'title_1', 'release_date': datetime.date(2020, 1, 10)},
        {'title': 'title_2', 'release_date': datetime.date(2020, 1, 11)}
    ]
    assert error_rows == [
        {'_errors': '{"row": {"release_date": "2020-3-11", "title": "title_2", "__count__title": 2}, '
                    '"errors": ["duplicate row"]}'},
        {'_errors': '{"row": {"release_date": "2020-1-51", "title": "title_3", "__count__title": 1}, '
                    '"errors": {"release_date": ["Not a valid date."]}}'}
    ]

    # Compound unique column test
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10"},
        {"title": "title_2", "release_date": "2020-1-11"},
        {"title": "title_2", "release_date": "2020-3-11"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_4", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = [["title", "release_date"]]

        title = fields.Str()
        release_date = fields.Date()

    valid_df, errors_df = TestSchema().validate_df(input_df)
    valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    error_rows = [row.asDict(recursive=True) for row in errors_df.collect()]
    assert valid_rows == [
        {'title': 'title_1', 'release_date': datetime.date(2020, 1, 10)},
        {'title': 'title_2', 'release_date': datetime.date(2020, 1, 11)},
        {'title': 'title_2', 'release_date': datetime.date(2020, 3, 11)},
        {'title': 'title_3', 'release_date': datetime.date(2020, 1, 21)}
    ]
    assert error_rows == [
        {'_errors': '{"row": {"release_date": "2020-1-21", "title": "title_3", "__count__title~release_date": 2}, '
                    '"errors": ["duplicate row"]}'},
        {'_errors': '{"row": {"release_date": "2020-1-51", "title": "title_4", "__count__title~release_date": 1}, '
                    '"errors": {"release_date": ["Not a valid date."]}}'}
    ]

    # Multiple unique columns test
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10"},
        {"title": "title_2", "release_date": "2020-1-11"},
        {"title": "title_2", "release_date": "2020-3-11"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_4", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = ["title", "release_date"]

        title = fields.Str()
        release_date = fields.Date()

    valid_df, errors_df = TestSchema().validate_df(input_df)
    valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    error_rows = [row.asDict(recursive=True) for row in errors_df.collect()]
    assert valid_rows == [
        {'title': 'title_1', 'release_date': datetime.date(2020, 1, 10)},
        {'title': 'title_2', 'release_date': datetime.date(2020, 1, 11)},
        {'title': 'title_3', 'release_date': datetime.date(2020, 1, 21)}
    ]
    assert error_rows == [
        {'_errors': '{"row": {"release_date": "2020-1-21", "title": "title_3", '
                    '"__count__title": 2, "__count__release_date": 2}, '
                    '"errors": ["duplicate row"]}'},
        {'_errors': '{"row": {"release_date": "2020-1-51", "title": "title_4", '
                    '"__count__title": 1, "__count__release_date": 1}, '
                    '"errors": {"release_date": ["Not a valid date."]}}'},
        {'_errors': '{"row": {"release_date": "2020-3-11", "title": "title_2", '
                    '"__count__title": 2, "__count__release_date": 1}, '
                    '"errors": ["duplicate row"]}'}
    ]


def test_validate_df_invalid_unique(spark_session):
    # Single unique column test
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10"},
        {"title": "title_2", "release_date": "2020-1-11"},
        {"title": "title_2", "release_date": "2020-3-11"},
        {"title": "title_3", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = ["title_fake"]

        title = fields.Str()
        release_date = fields.Date()

    with pytest.raises(ValueError):
        TestSchema().validate_df(input_df)

    # Compound unique column test
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10"},
        {"title": "title_2", "release_date": "2020-1-11"},
        {"title": "title_2", "release_date": "2020-3-11"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_4", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = [["title", "date"]]

        title = fields.Str()
        release_date = fields.Date()

    with pytest.raises(ValueError):
        TestSchema().validate_df(input_df)

    # Multiple unique columns test
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10"},
        {"title": "title_2", "release_date": "2020-1-11"},
        {"title": "title_2", "release_date": "2020-3-11"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_3", "release_date": "2020-1-21"},
        {"title": "title_4", "release_date": "2020-1-51"},
    ]
    input_df = spark_session.createDataFrame(input_data)

    class TestSchema(Schema):
        UNIQUE = ["title", "_date"]

        title = fields.Str()
        release_date = fields.Date()

    with pytest.raises(ValueError):
        TestSchema().validate_df(input_df)


def test_row_validator():
    input_data = [
        {"title": "valid_1", "release_date": "2020-1-10", "timestamp": datetime.datetime(2021, 5, 5)},
        {"title": "valid_2", "release_date": "2020-1-11", "timestamp": datetime.datetime(2021, 5, 5)},
        {"title": "invalid_1", "release_date": "2020-31-11", "timestamp": datetime.datetime(2021, 5, 5)},
        {"title": "invalid_2", "release_date": "2020-1-51", "timestamp": datetime.datetime(2021, 5, 5)},
    ]

    class TestSchema(Schema):
        title = fields.Str()
        release_date = fields.Date()
        timestamp = Raw(spark_type=DateType())

    validator = _RowValidator(TestSchema(), DEFAULT_ERRORS_COLUMN, [])
    validated_data = [validator.validate_row(Row(**x)) for x in input_data]
    for row in validated_data:
        if '_errors' in row:
            row['_errors'] = json.loads(row['_errors'])
    assert validated_data == [
        {
            'release_date': datetime.date(2020, 1, 10),
            'timestamp': datetime.datetime(2021, 5, 5, 0, 0),
            'title': 'valid_1'
        },
        {
            'release_date': datetime.date(2020, 1, 11),
            'timestamp': datetime.datetime(2021, 5, 5, 0, 0),
            'title': 'valid_2'
        },
        {'_errors': {"row": {
            "release_date": "2020-31-11",
            'timestamp': '2021-05-05 00:00:00',
            "title": "invalid_1"
        },
            "errors": {"release_date": ["Not a valid date."]}}},
        {'_errors': {"row": {
            "release_date": "2020-1-51",
            'timestamp': '2021-05-05 00:00:00',
            "title": "invalid_2"
        },
            "errors": {"release_date": ["Not a valid date."]}}}
    ]


def test_row_validator_with_duplicates():
    input_data = [
        {"title": "title_1", "release_date": "2020-1-10", '__count__title': 1},
        {"title": "title_2", "release_date": "2020-1-11", '__count__title': 1},
        {"title": "title_2", "release_date": "2020-3-11", '__count__title': 2},
        {"title": "title_3", "release_date": "2020-1-51", '__count__title': 1},
    ]

    class TestSchema(Schema):
        UNIQUE = ["title"]

        title = fields.Str()
        release_date = fields.Date()

    validator = _RowValidator(TestSchema(), DEFAULT_ERRORS_COLUMN, TestSchema.UNIQUE)
    validated_data = [validator.validate_row(Row(**x)) for x in input_data]
    for row in validated_data:
        if '_errors' in row:
            row['_errors'] = json.loads(row['_errors'])
    assert validated_data == [
        {'release_date': datetime.date(2020, 1, 10), 'title': 'title_1'},
        {'release_date': datetime.date(2020, 1, 11), 'title': 'title_2'},
        {'_errors': {"row": {"__count__title": 2, "release_date": "2020-3-11", "title": "title_2"},
                     "errors": ["duplicate row"]}},
        {'_errors': {"row": {"__count__title": 1, "release_date": "2020-1-51", "title": "title_3"},
                     "errors": {"release_date": ["Not a valid date."]}}}
    ]
