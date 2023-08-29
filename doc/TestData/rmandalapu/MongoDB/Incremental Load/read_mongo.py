# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""
from dependencies.MongoDB_audit import incremental_map
from dependencies.schema_utilties import get_mongo_options, get_mongo_reader
from dependencies.ConvertflatrenJson import flatJson

def mongo_reader(
        uri,
        database,
        collection,
        selectCols,
        filter,
        incr_col,
        incr_load_flg,
        audit_table_name

):

    options = get_mongo_options(
        uri,
        database,
        collection,
        selectCols
    )
    if incr_load_flg is not None:
        dataframe= incremental_map(uri,
                                   database,
                                   collection,
                                   selectCols,
                                   incr_col,
                                   incr_load_flg,
                                   audit_table_name)
    else:
        reader = get_mongo_reader(
        options
        )

        if filter.lower() == "none":
            dataframe = reader.load().select(selectCols)
        else:
            dataframe = reader.load().select(selectCols).filter(filter)
        try:
            dataframe=flatJson(dataframe)
        except:
            print("Unable to convert nested json")

    return dataframe
