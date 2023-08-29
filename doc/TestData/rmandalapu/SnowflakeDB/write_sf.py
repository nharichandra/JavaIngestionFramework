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
from dependencies.schema_utilties import get_sf_options

def sf_writer(
        dataframe:DataFrame,
        sfURL,
        sfAccount,
        sfUser,
        sfPassword,
        sfDatabase,
        sfSchema,
        sfRole,
        dbtable,
        mode
):

    writer = dataframe.write.format("net.snowflake.spark.snowflake").mode(mode)

    sfOptions = get_sf_options(
        sfURL,
        sfAccount,
        sfUser,
        sfPassword,
        sfDatabase,
        sfSchema,
        sfRole,
        dbtable
    )

    writer = writer.options(**sfOptions)

    writer.save()