# marshmallow-pyspark

[![Build Status](https://travis-ci.com/ketgo/marshmallow-pyspark.svg?token=oCVxhfjJAa2zDdszGjoy&branch=master)](https://travis-ci.com/ketgo/marshmallow-pyspark)
[![codecov.io](https://codecov.io/gh/ketgo/marshmallow-pyspark/coverage.svg?branch=master)](https://codecov.io/gh/ketgo/marshmallow-pyspark/coverage.svg?branch=master)
[![Apache 2.0 licensed](https://img.shields.io/badge/License-Apache%202.0-yellow.svg)](https://raw.githubusercontent.com/ketgo/marshmallow-pyspark/master/LICENSE)

[Marshmallow](https://marshmallow.readthedocs.io/en/stable/) is a popular package used for data serialization and validation. 
One defines data schemas in marshmallow containing rules on how input data should be marshalled. Similar to marshmallow, 
[pyspark](https://spark.apache.org/docs/latest/api/python/index.html) also comes with its own schema definitions used to 
process data frames. This package enables users to utilize marshmallow schemas and its powerful data validation capabilities 
in pyspark applications. Such capabilities can be utilized in data-pipeline ETL jobs where data consistency and quality 
is of importance.

## Install

The package can be install using `pip`:
```bash
$ pip install marshmallow-pyspark
```

## Usage

Data schemas can can define the same way as you would using marshmallow. A quick example is shown below:
```python
from marshmallow_pyspark import Schema
from marshmallow import fields

# Create data schema.
class AlbumSchema(Schema):
    title = fields.Str()
    release_date = fields.Date()

# Input data frame to validate.
df = spark.createDataFrame([
    {"title": "valid_1", "release_date": "2020-1-10"},
    {"title": "valid_2", "release_date": "2020-1-11"},
    {"title": "invalid_1", "release_date": "2020-31-11"},
    {"title": "invalid_2", "release_date": "2020-1-51"},
])

# Get data frames with valid rows and error prone rows 
# from input data frame by validating using the schema.
valid_df, errors_df = AlbumSchema().validate_df(df)

# Output of valid data frame
valid_df.show()
#    +-------+------------+
#    |  title|release_date|
#    +-------+------------+
#    |valid_1|  2020-01-10|
#    |valid_2|  2020-01-11|
#    +-------+------------+

# Output of errors data frame
errors_df.show()
#    +--------------------+
#    |             _errors|
#    +--------------------+
#    |{"row": {"release...|
#    |{"row": {"release...|
#    +--------------------+
```

### More Options

On top of marshmallow supported options, the `Schema` class comes with two additional initialization arguments:

- `error_column_name`: name of the column to store validation errors. Default value is `_errors`.

- `split_errors`: split rows with validation errors as a separate data frame from valid rows. When set to `False` the 
   rows with errors are returned together with valid rows as a single data frame. The field values of all error rows are 
   set to `null`. For user convenience the original field values can be found in the `row` attribute of the error JSON. 
   Default value is `True`. 

An example is shown below:
```python
from marshmallow import EXCLUDE

schema = AlbumSchema(
    error_column_name="custom_errors",     # Use 'custom_errors' as name for errors column
    split_errors=False,                     # Don't split the input data frame into valid and errors
    unkown=EXCLUDE                          # Marshmallow option to exclude fields not present in schema
)

# Input data frame to validate.
df = spark.createDataFrame([
    {"title": "valid_1", "release_date": "2020-1-10", "garbage": "wdacfa"},
    {"title": "valid_2", "release_date": "2020-1-11", "garbage": "5wacfa"},
    {"title": "invalid_1", "release_date": "2020-31-11", "garbage": "3aqf"},
    {"title": "invalid_2", "release_date": "2020-1-51", "garbage": "vda"},
])

valid_df, errors_df = schema.validate_df(df)

# Output of valid data frame. Contains rows with errors as
# the option 'split_errors' was set to False.
valid_df.show()
#    +-------+------------+--------------------+
#    |  title|release_date|             _errors|
#    +-------+------------+--------------------+
#    |valid_1|  2020-01-10|                    |
#    |valid_2|  2020-01-11|                    |
#    |       |            |{"row": {"release...|
#    |       |            |{"row": {"release...|
#    +-------+------------+--------------------+

# The errors data frame will be set to None
assert errors_df is None        # True
```

Lastly, on top of passing marshmallow specific options in the schema, you can also pass them in the `validate_df` method.
These are options are passed to the marshmallow's `load` method:
```python
schema = AlbumSchema(
    error_column_name="custom_errors",     # Use 'custom_errors' as name for errors column
    split_errors=False,                     # Don't split the input data frame into valid and errors
)

valid_df, errors_df = schema.validate_df(df, unkown=EXCLUDE)
```

### Fields

Marshmallow comes with a variety of different fields that can be used to define schemas. Internally marshmallow-pyspark 
convert these fields into pyspark SQL data types. The following table lists the supported marshmallow fields and their 
equivalent spark SQL data types:


| Marshmallow | PySpark |
| --- | --- |
| `String` | `StringType` |
| `DateTime` | `TimestampType` |
| `Date` | `DateType` |
| `Boolean` | `BooleanType` |
| `Integer` | `IntegerType` |
| `Float` | `FloatType` |
| `Number` | `DoubleType` |
| `List` | `ArrayType` |
| `Dict` | `MapType` |
| `Nested` | `StructType` |

By default the `StringType` data type is used for marshmallow fields not in the above table.

#### Custom Fields

It is also possible to add support for custom marshmallow fields, or those missing in the above table. In order to do so, 
you would need to create a converter for the custom field. The converter can be built using the `ConverterABC` interface:
```python
from marshmallow_pyspark import ConverterABC
from pyspark.sql.types import StringType


class EmailConverter(ConverterABC):
    """
        Converter to convert marshmallow's Email field to a pyspark 
        SQL data type.
    """

    def convert(self, ma_field):
        return StringType()
```  
The `ma_field` argument in the `convert` method is provided to handle nested fields. For an example you can checkout 
`NestedConverter`. Now the final step would be to add the converter to the `CONVERTER_MAP` attribute of your schema:
```python
from marshmallow_pyspark import Schema
from marshmallow import fields


class User(Schema):
    name = fields.String(required=True)
    email = fields.Email(required=True)

# Adding email converter to schema.
User.CONVERTER_MAP[fields.Email] = EmailConverter

# You can now use your schema to validate the input data frame.
valid_df, errors_df = User().validate_df(input_df)
```


## Milestones

Most valuable features to be implemented in the order of importance:

- [ ] Validation for unique valued fields
- [ ] Support marshmallow function and method fields

## Development

To hack marshmallow-pyspark locally run:

```bash
$ pip install -e .[dev]			# to install all dependencies
$ pytest --cov-config .coveragerc --cov=./			# to get coverage report
$ pylint marshmallow_pyspark			# to check code quality with PyLint
```

Optionally you can use `make` to perform development tasks.

## License

The source code is licensed under Apache License Version 2.

## Contributions

Pull requests always welcomed! :)
