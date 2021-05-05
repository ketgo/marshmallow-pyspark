"""
    Class for defining data schemas
"""

import json
from typing import Union, Dict, Tuple, List, Type

from marshmallow import Schema as ma_Schema, fields as ma_fields, ValidationError
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import udf, struct, row_number
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

from .constants import (DEFAULT_ERRORS_COLUMN, COMBINATION_COLUMN_SEP,
                        COUNT_COLUMN_TEMPLATE, DEFAULT_SPLIT_ERRORS)
from .converters import (ConverterABC, StringConverter, DateTimeConverter,
                         DateConverter, BooleanConverter, FloatConverter,
                         IntegerConverter, NumberConverter, ListConverter,
                         DictConverter, NestedConverter, RawConverter)
from .fields import Raw


# This class is added to support unit testing of UDF
class _RowValidator:
    """
        Row validator class to validate data frame rows. This class
        is used for internal purposes only.

        :param schema: schema class instance
        :param error_column_name: error column name to use
        :param unique_fields: list of unique fields to check
        :param args, kwargs: arguments passed to marshmallow load method
    """

    def __init__(
            self,
            schema: "Schema",
            error_column_name: str,
            unique_fields: List[Union[str, List[str]]],
            *args, **kwargs
    ):
        self._schema = schema
        self._error_column_name = error_column_name
        self._unique_fields = unique_fields
        self._args = args
        self._kwargs = kwargs

        # Creating duplicate count column names
        self._count_columns = []
        for field in self._unique_fields:
            column = [field] if isinstance(field, str) else field
            count_column = COUNT_COLUMN_TEMPLATE.format(COMBINATION_COLUMN_SEP.join(column))
            self._count_columns.append(count_column)

    def validate_row(self, row: Row) -> Dict:
        """
            Validate data frame row
        """
        data = row.asDict(recursive=True)
        schema_data = {key: value for key, value in data.items() if key not in self._count_columns}
        duplicate_counts_data = [data[column] for column in self._count_columns]
        try:
            # Validate schema using marshmallow
            rvalue = self._schema.load(schema_data, *self._args, **self._kwargs)
            # Validate uniqueness
            if sum(duplicate_counts_data) > len(duplicate_counts_data):
                raise ValidationError("duplicate row")
        except ValidationError as err:
            # Return errors
            rvalue = {
                self._error_column_name: json.dumps(
                    {
                        "row": data,
                        "errors": err.messages,
                    },
                    default=str,
                )
            }

        return rvalue


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
                {"title": "valid_1", "release_date": "2020-1-10"},
                {"title": "valid_2", "release_date": "2020-1-11"},
                {"title": "invalid_1", "release_date": "2020-31-11"},
                {"title": "invalid_2", "release_date": "2020-1-51"},
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
            #    |{"row": {"release...|
            #    |{"row": {"release...|
            #    +--------------------+

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

    #: Map of marshmallow field types and corresponding converters
    CONVERTER_MAP: Dict[Type[ma_fields.Field], Type[ConverterABC]] = {
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
        Raw: RawConverter,
    }

    #: List of unique valued single or combination of schema fields
    UNIQUE: List[Union[str, List[str]]] = []

    def __init__(
            self,
            error_column_name: Union[str, bool] = None,
            split_errors: bool = None,
            *args, **kwargs
    ):
        self.error_column_name = DEFAULT_ERRORS_COLUMN \
            if error_column_name is None else error_column_name
        self.split_errors = DEFAULT_SPLIT_ERRORS if split_errors is None else split_errors
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

    # pylint: disable=invalid-name
    def validate_df(
            self,
            df: DataFrame,
            *args, **kwargs
    ) -> Tuple[DataFrame, Union[DataFrame, None]]:
        """
            Method to validate pyspark data frame.

            :param df: pyspark data frame object to validate
            :param args, kwargs: additional arguments passed to marshmallows` load function
            :returns: Tuple of data frames for valid rows and errors
        """
        # Add duplicate counts for unique fields
        _df = self._add_duplicate_counts(df)
        # Create row validator
        row_validator = _RowValidator(self, self.error_column_name, self.UNIQUE, *args, **kwargs)
        # PySpark UDF for serialization
        _validate_row_udf = udf(row_validator.validate_row, returnType=self.spark_schema)
        # Validate each row in data frame
        _df: DataFrame = _df.withColumn(
            "fields",
            _validate_row_udf(struct(*_df.columns))
        ).select("fields.*")

        if self.split_errors:
            # Cache date to avoid re-run of validation for errors data frame
            _df.cache()
            # Split data frame into valid and invalid rows
            valid_rows_df = _df.where(
                _df[self.error_column_name].isNull()
            ).drop(self.error_column_name)
            errors_df = _df.select(
                self.error_column_name
            ).where(_df[self.error_column_name].isNotNull())
        else:
            valid_rows_df = _df
            errors_df = None

        return valid_rows_df, errors_df

    # pylint: disable=invalid-name
    def _add_duplicate_counts(self, df: DataFrame) -> DataFrame:
        """
            Add duplicate counts for unique fields

            :param df: data frame to add counts
            :return: data frame object
        """
        rvalue = df
        for unique_fields in self.UNIQUE:
            # Getting columns to check duplicates
            columns = [unique_fields] if isinstance(unique_fields, str) else unique_fields
            self.__check_field_name(columns)

            # Using window function for checking duplicates
            count_column = COUNT_COLUMN_TEMPLATE.format(COMBINATION_COLUMN_SEP.join(columns))
            window = Window.partitionBy(columns).orderBy(columns)
            rvalue = rvalue.withColumn(count_column, row_number().over(window))

        return rvalue

    def __check_field_name(self, field_names: List[str]):
        for field_name in field_names:
            if field_name not in self._declared_fields:
                raise ValueError("Field '{}' not found in schema.".format(field_name))
