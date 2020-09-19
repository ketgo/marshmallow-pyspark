"""
    Custom fields unit tests
"""

from datetime import date

from pyspark.sql.types import DateType

from marshmallow_pyspark.fields import Raw
from marshmallow_pyspark.schema import Schema


def test_raw_field(spark_session):
    schema_dict = {
        "date": Raw(spark_type=DateType())
    }
    mock = Schema.from_dict(schema_dict)
    data = {
        "date": date(2020, 9, 12)
    }
    assert data == mock().load(data)
    assert isinstance(schema_dict["date"].spark_type, DateType)

    input_data = [
        {"date": date(2020, 1, 10)},
        {"date": date(2020, 1, 11)},
    ]
    # Input data frame to validate.
    df = spark_session.createDataFrame(input_data)

    # Get data frames with valid rows and error prone rows
    # from input data frame by validating using the schema.
    valid_df, errors_df = mock().validate_df(df)
    valid_rows = [row.asDict(recursive=True) for row in valid_df.collect()]
    error_rows = [row.asDict(recursive=True) for row in errors_df.collect()]
    assert valid_rows == input_data
    assert error_rows == []
