# marshmallow-pyspark

[Marshmallow]() is a popular package used for data serialization and validation. One defines data schemas in marshmallow 
containing rules on how input data should be marshalled. Similar to marshmallow, [pyspark]() also comes with its own schema 
definitions used to process data frames. This package enables users to utilize marshmallow schemas and its powerful data
 validation capabilities in pyspark applications. Such capabilities can be utilized in data-pipeline ETL jobs where data 
 consistency and quality is of importance.

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
TODO

### Custom Fields
TODO

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
