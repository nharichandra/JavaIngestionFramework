# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""
from dependencies.schema_utilties import get_file_reader, get_options

def file_reader(
        path,
        data_format,
        schema_file,
        num_partittions,
        selectCol,
        filter
):
    options = get_options(
        data_format.header,
        data_format.delimiter,
        data_format.inferSchema,
        data_format.multiLine,
        data_format.roottag,
        data_format.rowtag
    )
    reader = get_file_reader(
        schema_file,
        options
    )

    file_format = str(data_format.dataFormat).lower()

    if file_format == 'csv':
        if filter.lower() == "none":
            dataframe = reader.csv(path).select(selectCol)
        else:
            dataframe = reader.csv(path).select(selectCol).filter(filter)
    elif file_format == "json":
        if filter.lower() == "none":
            dataframe = reader.json(path).select(selectCol)
        else:
            dataframe = reader.json(path).select(selectCol).filter(filter)
    elif file_format == "parquet":
        if filter.lower() == "none":
            dataframe = reader.parquet(path).select(selectCol)
        else:
            dataframe = reader.parquet(path).select(selectCol).filter(filter)
    elif file_format == "delta":
        if filter.lower() == "none":
            dataframe = reader.format("delta").load(path).select(selectCol)
        else:
            dataframe = reader.format("delta").load(path).select(selectCol).filter(filter)
    elif file_format == "xml":
        if filter.lower() == "none":
            dataframe = reader.format("com.databricks.spark.xml").load(path).select(selectCol)
        else:
            dataframe = reader.format("com.databricks.spark.xml").load(path).select(selectCol).filter(filter)
    elif file_format == "fixed":
        if filter.lower() == "none":
            dataframe = read_fixed_file(path, data_format, num_partittions).select(selectCol)
        else:
            dataframe = read_fixed_file(path, data_format, num_partittions).select(selectCol).filter(filter)
    elif file_format == 'avro' :
        """
        need to develop funcationality
        """
        dataframe = reader.format("avro").load(path)
        pass
    else:
        raise Exception(" Unknown data format! ")

    if num_partittions > 0:
        dataframe = dataframe.repartition(num_partittions)

    return dataframe


def read_fixed_file(
        path,
        data_format,
        num_partitions
):
    from dependencies.spark import spark_session
    from dependencies.schema_utilties import data_type_dict
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    import numpy

    if data_format.colNameTypeLength is None:
        raise Exception(" Missing fixed-length file parameters")

    column_length = []
    column_name = []
    column_datatype = []

    for col in data_format.colNameTypeLength:
        column_length.append(col.columnLength)
        column_datatype.append(col.columnType)
        column_name.append(col.columnName)

    cumulative_length = numpy.cumsum([1] + column_length)

    dataframe = spark_session.read.text(path).withColumnRenamed("value", "stringColumn")

    if data_format.header:
        initial_record = str(dataframe.first()[0])
        dataframe = dataframe.filter(F.col("stringColumn") != initial_record)

    if num_partitions > 0:
        dataframe = dataframe.repartition(num_partitions)

    for columns in zip(column_name, cumulative_length.tolist(), column_length):
        dataframe = dataframe.withColumn(
            columns[0],
            F.trim(
                F.substring(
                    F.col("stringColumn"),
                    columns[1],
                    columns[2]
                )
            )
        )

    dataframe = dataframe.drop(F.col("stringColumn"))

    for columns in zip(column_name, column_datatype):
        if columns[1] != "str":
            dataframe = dataframe.withColumn(
                columns[0],
                F.col(columns[0]).cast(data_type_dict.get(columns[1], StringType()))

            )

    return dataframe
