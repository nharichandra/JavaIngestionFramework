# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""
from dependencies.sfdb_audit import sfdb_audit_query
from dependencies.schema_utilties import get_sf_options, get_sf_reader

def sf_reader(
        sfURL,
        sfAccount,
        sfUser,
        sfPassword,
        sfDatabase,
        sfSchema,
        sfRole,
        query,
        selectCol,
        filter,
        sfdbIncremental
):
    if sfdbIncremental is not None:
        query, max_value = sfdb_audit_query(
            sfURL,
            sfAccount,
            sfUser,
            sfPassword,
            sfDatabase,
            sfSchema,
            sfRole,
            query,
            selectCol,
            sfdbIncremental
        )
    else:
        query = query
        max_value = None

    sfOptions = get_sf_options(
        sfURL,
        sfAccount,
        sfUser,
        sfPassword,
        sfDatabase,
        sfSchema,
        sfRole,
        query
    )

    reader = get_sf_reader(
        sfOptions
    )

    if filter.lower() == "none":
        dataframe = reader.load().select(selectCol)
    else:
        dataframe = reader.load().select(selectCol).filter(filter)

    return dataframe, max_value
