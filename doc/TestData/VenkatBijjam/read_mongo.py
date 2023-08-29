# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""
from dependencies.db_audit import db_audit_query
from dependencies.schema_utilties import get_mongo_options, get_mongo_reader

def mongo_reader(
        uri,
        database,
        collection,
        selectCol,
        filter
):

    options = get_mongo_options(
        uri,
        database,
        collection
    )

    reader = get_mongo_reader(
        options
    )

    if filter.lower() == "none":
        dataframe = reader.load().select(selectCol)
    else:
        dataframe = reader.load().select(selectCol).filter(filter)

    return dataframe
