"""
    Custom marshmallow field types
"""

from marshmallow import fields
from pyspark.sql.types import DataType


class Raw(fields.Raw):
    """
        Field that applies no formatting and uses passed
        pyspark data type during marshalling of the field.

        Example:

        .. code-block:: python

            from marshmallow_pyspark import Schema
            from marshmallow_pyspark.fields import Raw
            from marshmallow import fields
            from pyspark.sql.types import DateType
            from datetime import date

            class AlbumSchema(Schema):
                title = fields.Str()
                release_date = Raw(spark_type=DateType())

            df = spark.createDataFrame([
                {"title": "valid_1", "release_date": date(2020, 1, 10)},
                {"title": "valid_2", "release_date": date(2020, 1, 11)},
            ])

            valid_df, errors_df = AlbumSchema().validate_df(df)

            valid_df.show()
            #    +-------+------------+
            #    |  title|release_date|
            #    +-------+------------+
            #    |valid_1|  2020-01-10|
            #    |valid_2|  2020-01-11|
            #    +-------+------------+

            errors_df.show()
            #    +--------------------+
            #    |             _errors|
            #    +--------------------+
            #    +--------------------+

        :param spark_type: spark data type to use when marshalling the field
    """

    def __init__(self, spark_type: DataType, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._spark_type = spark_type

    @property
    def spark_type(self) -> DataType:
        return self._spark_type
