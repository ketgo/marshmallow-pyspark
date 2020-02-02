"""
    Class for defining data schemas
"""

import json
from typing import *

from marshmallow import Schema as ma_Schema, fields as ma_fields, ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StructType, StructField, StringType

from .constants import *
from .converters import (ConverterABC, StringConverter, DateTimeConverter, DateConverter, BooleanConverter,
                         FloatConverter, IntegerConverter, NumberConverter, ListConverter, DictConverter,
                         NestedConverter)


class Schema(ma_Schema):
    """
        Schema class used for validating pyspark data frames.

        Example usage:

        .. code-block:: python

            from marshmallow_pyspark import Schema
            from marshmallow import fields


            class AlbumSchema(Schema):
                title = fields.Str()
                release_date = fields.Date()

            df = spark.createDataFrame([
                {"title": "", "release_date": ""},
            ])
            valid_df, errors_df = AlbumSchema()

            valid_df.show()
            # Data: TODO: Add correct data
                +---------+-------------+
                |    title|release_date|
                +---------+---+--------+--------+
                |  valid_1| 40|   43.78|    true|
                |  valid_2| 32|    30.5|   false|
                |invalid_3| 32|    30.0|   false|
                +---------+---+--------+--------+

            errors_df.show()
            # Data: TODO: Add correct data
                +--------------------+
                |             _errors|
                +--------------------+
                |{"row": {"age": "...|
                |{"row": {"age": "...|
                +--------------------+

        :param error_column_name: name of the column to store validation errors.
            Default value is `_errors`.
        :param split_errors: split validation errors for invalid rows from
            valid rows data frame into a separate data frame. When set to
            `False` the invalid rows are returned together with valid rows
            as a single data frame. The field values of all invalid rows are
            then set to `null`. For user convenience the original field values
            can be found in the `row` attribute of the error JSON.
            Default value is `True`.
        :param args, kwargs: arguments passed to marshmallow schema class
    """

    # Map of marshmallow field types and corresponding converters
    CONVERTER_MAP: Mapping[Type[ma_fields.Field], Type[ConverterABC]] = {
        ma_fields.String: StringConverter,
        ma_fields.DateTime: DateTimeConverter,
        ma_fields.Date: DateConverter,
        ma_fields.Boolean: BooleanConverter,
        ma_fields.Integer: IntegerConverter,
        ma_fields.Float: FloatConverter,
        ma_fields.Number: NumberConverter,
        ma_fields.List: ListConverter,
        ma_fields.Dict: DictConverter,
        ma_fields.Nested: NestedConverter,
    }

    def __init__(
            self,
            error_column_name: Union[str, bool] = None,
            split_errors: bool = None,
            *args, **kwargs
    ):
        self.error_column_name = DEFAULT_ERRORS_COLUMN_NAME if not error_column_name else error_column_name
        self.split_errors = DEFAULT_SPLIT_INVALID_ROWS if split_errors is None else split_errors
        super().__init__(*args, **kwargs)

    @property
    def spark_schema(self) -> StructType:
        """
            Spark schema from marshmallow schema
        """
        fields = []
        for field_name, ma_field in self._declared_fields.items():
            field_converter = self.CONVERTER_MAP.get(type(ma_field), StringConverter)
            spark_field = field_converter(self.CONVERTER_MAP).convert(ma_field)
            fields.append(StructField(field_name, spark_field, nullable=True))
        # Adding error column field
        fields.append(StructField(self.error_column_name, StringType(), nullable=True))

        return StructType(fields)

    def validate_df(
            self,
            df: DataFrame,
            *args, **kwargs
    ) -> Tuple[DataFrame, Union[DataFrame, None]]:
        """
            Validate pyspark data frame.

            :param df: pyspark data frame object to validate
            :param args, kwargs: additional arguments passed to marshmallows` load function
            :returns: Tuple of data frames for valid rows and errors
        """

        # PySpark UDF for serialization
        @udf(returnType=self.spark_schema)
        def _deserialize_row_udf(row):
            """
                Deserialize Row object
            """
            data = row.asDict(recursive=True)
            try:
                rvalue = self.load(data, *args, **kwargs)
            except ValidationError as err:
                rvalue = {
                    self.error_column_name: json.dumps(
                        {
                            "row": data,
                            "errors": err.messages,
                        }
                    )
                }

            return rvalue

        _df: DataFrame = df.withColumn(
            "fields",
            _deserialize_row_udf(struct(*df.columns))
        ).select("fields.*")

        if self.split_errors:
            # Split data frame into valid and invalid rows
            valid_rows_df = _df.where(_df[self.error_column_name].isNull()).drop(self.error_column_name)
            errors_df = _df.select(self.error_column_name).where(_df[self.error_column_name].isNotNull())
        else:
            valid_rows_df = _df
            errors_df = None

        return valid_rows_df, errors_df
