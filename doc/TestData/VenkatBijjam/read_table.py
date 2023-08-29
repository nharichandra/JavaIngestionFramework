# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""
from dependencies.db_audit import db_audit_query
from dependencies.schema_utilties import get_jdbc_options, get_jdbc_reader

def table_reader(
        driver,
        url,
        user,
        password,
        dbtable,
        selectCol,
        filter,
        dbIncremental,
        num_partitions,
        partition_column,
        lower_bound,
        upper_bound,
        query_timeout,
        fetch_size,
        predicate_pushdown
):
    if dbIncremental is not None:
        query, max_value = db_audit_query(
            url,
            user,
            password,
            driver,
            dbtable,
            dbIncremental
        )
    else:
        query = dbtable
        max_value = None

    options = get_jdbc_options(
        url,
        user,
        password,
        driver,
        query,
        num_partitions,
        partition_column,
        lower_bound,
        upper_bound,
        query_timeout,
        fetch_size,
        predicate_pushdown
    )

    reader = get_jdbc_reader(
        options
    )

    if filter.lower() == "none":
        dataframe = reader.load().select(selectCol)
    else:
        dataframe = reader.load().select(selectCol).filter(filter)

    return dataframe, max_value
