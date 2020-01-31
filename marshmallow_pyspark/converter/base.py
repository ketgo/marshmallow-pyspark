"""
    Base class for converter
"""

from abc import ABCMeta, abstractmethod
from typing import Mapping, Type

from marshmallow.fields import Field
from pyspark.sql.types import DataType


class ConverterABC(metaclass=ABCMeta):
    """
        Abstract base class for converter

        :param converter_map: mapping between marshmallow field and
            corresponding converter. Primarily used by nested composite
            fields like list, mapping, etc.
    """

    def __init__(self, converter_map: Mapping[Type[Field], Type["ConverterABC"]]):
        self._converter_map = converter_map

    @property
    def converter_map(self) -> Mapping[Type[Field], Type["ConverterABC"]]:
        return self._converter_map

    @abstractmethod
    def convert(self, ma_field: Field) -> DataType:
        """
            Convert marshmallow field to spark data type.

            :param ma_field: marshmallow field instance
            :return: spark SQL data type instance
        """
        raise NotImplementedError()
