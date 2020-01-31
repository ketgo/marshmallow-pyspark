"""
    Schema class for defining marshalling schemas
"""

import json
from typing import *

from marshmallow import Schema as ma_Schema, ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *

from .constants import *
from .converter import Converter


class Schema(ma_Schema):
    """
        PySpark schema class

        :param error_column_name: name of the column to store validation errors.
            Default value is `errors`.
        :param split_invalid_rows: split invalid rows as separate data frame.
            Default value is `False`.
        :param *args, **kwargs: arguments passed to marshmallow schema class
    """

    # Field converter used in creating spark schema
    _converter = Converter()

    def __init__(
            self,
            error_column_name: Union[str, bool] = None,
            split_invalid_rows: bool = None,
            *args, **kwargs
    ):
        # Set validation errors field
        self.error_column_name = DEFAULT_ERRORS_COLUMN_NAME if error_column_name is None else error_column_name
        # Flag to split invalid rows
        self.split_invalid_rows = DEFAULT_SPLIT_INVALID_ROWS if split_invalid_rows is None else split_invalid_rows
        # Initialize base schema class
        super().__init__(*args, **kwargs)

    @property
    def spark_schema(self) -> StructType:
        """
            Spark schema from marshmallow schema
        """
        fields = []
        for field_name, ma_field in self._declared_fields.items():
            fields.append(StructField(field_name, self._converter.convert(ma_field), nullable=True))
        # Adding error column field
        if self.error_column_name:
            fields.append(StructField(self.error_column_name, StringType(), nullable=True))

        return StructType(fields)

    def load_df(
            self,
            df: DataFrame,
            *args, **kwargs
    ) -> Tuple[DataFrame, Union[DataFrame, None]]:
        @udf(returnType=self.spark_schema)
        def _deserialize_row_udf(row):
            """
                Deserialize Row object
            """
            data = row.asDict(recursive=True)
            try:
                rvalue = self.load(data, *args, **kwargs)
            except ValidationError as err:
                rvalue = data
                rvalue["is_valid"] = False
                rvalue["errors"] = json.dumps(err.messages)
            return rvalue

        _df: DataFrame = df.withColumn(
            "fields",
            _deserialize_row_udf(struct(*df.columns))
        ).select("fields.*")
        # Split data frame into valid and invalid rows
        valid_rows_df = _df.where(_df["is_valid"])
        invalid_rows_df = _df.where(~_df["is_valid"])

        return valid_rows_df, invalid_rows_df

    def dump_df(
            self,
            df: DataFrame,
            *args, **kwargs
    ) -> Tuple[DataFrame, Union[DataFrame, None]]:
        pass
