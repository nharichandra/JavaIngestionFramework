# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""

from pyspark.sql.types import *

data_type_dict = {
    "int": IntegerType(),
    "str": StringType(),
    "double": DoubleType(),
    "long": LongType(),
    "float": FloatType(),
    "decimal": DecimalType(),
    "date": DateType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType()
}

def get_options(
        header: bool = None,
        delimiter: str = None,
        inferschema: bool = None,
        multiline: bool = None,
        rootag: str = None,
        rowtag: str = None
):
    options = {}

    if header is not None:        options['header'] = header
    if delimiter is not None:        options['delimiter'] = delimiter
    if inferschema is not None:        options['inferschema'] = inferschema
    if multiline is not None:        options['multiline'] = multiline
    if rootag is not None:        options['rootag'] = rootag
    if rowtag is not None:        options['rowtag'] = rowtag

    return options


def get_schema(
        schema_file: str
):
    from pyspark.sql.types import StructType
    import json

    with open(schema_file) as schema_json_file:
        schema_value = json.load(schema_json_file)

    return StructType.fromJson(schema_value)


def get_jdbc_options(
        url: str,
        user: str,
        password: str,
        driver: str,
        dbtable: str,
        num_partitions: int,
        partition_column: str,
        lower_bound: str,
        upper_bound: str,
        query_timeout: int,
        fetch_size: int,
        predicate_pushdown: bool
):
    options = {}

    if url:        options['url'] = url
    if driver is not None:        options['driver'] = driver
    if user:        options['user'] = user
    if password:        options['password'] = password
    if dbtable is not None:        options['dbtable'] = dbtable
    if num_partitions > 0:        options['numPartitions'] = num_partitions
    if partition_column is not None:        options['partitionColumn'] = partition_column
    if lower_bound is not None:        options['lowerBound'] = lower_bound
    if upper_bound is not None:        options['upperBound'] = upper_bound
    if query_timeout > 0:        options['queryTimeout'] = query_timeout
    if fetch_size > 0:        options['fetchsize'] = fetch_size
    if predicate_pushdown:        options['pushDownPredicate'] = predicate_pushdown

    return options

def get_jdbc_writer_options(
        url: str,
        driver: str,
        user: str,
        password: str,
        dbtable: str
):
    writer_options = {}

    if url:        writer_options['url'] = url
    if driver is not None:        writer_options['driver'] = driver
    if user:        writer_options['user'] = user
    if password:        writer_options['password'] = password
    if dbtable is not None:        writer_options['dbtable'] = dbtable

    return writer_options

def get_mongo_options(
        uri: str,
        database: str,
        collection: str
):
    options = {}

    if uri:        options['uri'] = uri
    if database is not None:        options['database'] = database
    if collection:        options['collection'] = collection

    return options


def get_file_reader(
        schema_file: str,
        options: dict
):
    # from src.dependencies.spark import spark_session
    from dependencies.spark import spark_session
    reader = spark_session.read


    for option_name in options:
        reader = reader.option(option_name, options[option_name])

    if schema_file:
        struct_schema = get_schema(schema_file)
        reader = reader.schema(struct_schema)

    return reader


def get_jdbc_reader(
        options
):
    # from src.dependencies.spark import spark_session
    from dependencies.spark import spark_session
    reader = spark_session.read.format("jdbc")

    for option_name in options:
        reader = reader.option(option_name, options[option_name])

    print(reader)
    return reader

def get_mongo_reader(
        options
):
    # from src.dependencies.spark import spark_session
    from dependencies.spark import spark_session
    reader = spark_session.read.format("com.mongodb.spark.sql.DefaultSource")

    for option_name in options:
        reader = reader.option(option_name, options[option_name])

    print(reader)
    return reader