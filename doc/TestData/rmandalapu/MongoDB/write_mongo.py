# -*- coding: utf-8 -*-
"""
Created on 05/11/2020 4:31 PM

@author : Venkat Bijjam
"""
from typing import List

from pyspark.sql import DataFrame

# from src.config.config_objects import ColumnNameType
# from src.dependencies.dataframe_utils import dataframe_add_columns
# from src.dependencies.schema_utilties import get_options

from config.config_objects import ColumnNameTypeLength
from dependencies.dataframe_utils import dataframe_add_columns
from dependencies.db_audit import db_audit_query
from dependencies.schema_utilties import get_mongo_options

def mongo_writer(
        dataframe:DataFrame,
        uri,
        database,
        collection,
        mode,
        selectCols
):
    if selectCols:
        print(selectCols)
        dataframe = dataframe.select(selectCols)
    else:
        writer = dataframe.drop("_id").write.format("com.mongodb.spark.sql.DefaultSource").mode(mode)

        options = get_mongo_options(
            uri,
            database,
            collection,
            selectCols
        )
        for option_name in options:
            writer = writer.option(option_name, options[option_name])

        writer.save()