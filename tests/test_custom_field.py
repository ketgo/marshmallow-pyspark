"""
    Test custom field
"""

from datetime import date

from marshmallow import fields, ValidationError

from marshmallow_pyspark import Schema
from marshmallow_pyspark.converters import DateConverter


class DateObject(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):  # pragma: no cover
        if value is None:
            return None
        return str(value)

    def _deserialize(self, value, attr, data, **kwargs):  # pragma: no cover
        # The value is already a date object
        if isinstance(value, date):
            return value
        else:
            raise ValidationError("Not a valid datetime.")


# Create data schema.
class AlbumSchema(Schema):
    title = fields.Str()
    release_date = DateObject()


# Adding converter for dateobject to schema.
AlbumSchema.CONVERTER_MAP[DateObject] = DateConverter


def test_validate_df(spark_session):
    input_data = [
        {"title": "valid_1", "release_date": date(2020, 1, 10)},
        {"title": "valid_2", "release_date": date(2020, 1, 11)},
        {"title": "valid_3", "release_date": date(2020, 1, 11)},
        {"title": "valid_4", "release_date": date(2020, 1, 21)},
    ]

    # Input data frame to validate.
    df = spark_session.createDataFrame(input_data)

    # Get data frames with valid rows and error prone rows
    # from input data frame by validating using the schema.
    valid_df, errors_df = AlbumSchema().validate_df(df)
    valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    error_rows = [row.asDict(recursive=True) for row in errors_df.collect()]
    assert valid_rows == input_data
    assert error_rows == []
