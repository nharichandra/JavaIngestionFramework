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
from dependencies.schema_utilties import get_jdbc_writer_options

def table_writer(
        dataframe:DataFrame,
        url,
        driver,
        user,
        password,
        dbtable,
        mode
):

    writer = dataframe.write.format("jdbc").mode(mode)

    writer_options = get_jdbc_writer_options(
        url,
        driver,
        user,
        password,
        dbtable
    )

    for option_name in writer_options:
        writer = writer.option(option_name, writer_options[option_name])

    writer.save()