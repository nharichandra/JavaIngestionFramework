# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""

from dependencies.db_audit import db_audit_write
from dependencies.sfdb_audit import sfdb_audit_write
from dependencies.pretty_print import pretty_print
from publish.write_file import file_writer
from publish.WriteDB import dbms_writer
from publish.write_mongo import mongo_writer
from publish.write_sf import sf_writer

def batch_job(jobs_all):
    '''
    Batch job which consume File or Database input
    Data is loaded in DataFrame and Finally written as File Out
    :param jobs_all:
    :return:
    '''
    for job in jobs_all.jobs:
        pretty_print(" Initiating Batch Job -- " + str(job.name))

        dataframe_dict = {}
        dbms_audit_dict = {}

        # Identify type of input
        if job.fileInputs:
            for file_input in job.fileInputs:
                df = read_file(file_input)
                df.show(1, False)
                df.printSchema()
                dataframe_dict[file_input.dataFrameName] = df

        if job.dbmsInputs:
            for dbms_input in job.dbmsInputs:
                df, max_value = read_table(dbms_input)
                # df.show(1, False)
                if max_value is not None:
                    dbms_audit_dict[dbms_input.dataFrameName] = max_value
                dataframe_dict[dbms_input.dataFrameName] = df

        if job.mongoInputs:
            for mongo_input in job.mongoInputs:
                df = read_mongo(mongo_input)
                df.show(1, False)
                dataframe_dict[mongo_input.dataFrameName] = df
        if job.sfInputs:
            for sf_input in job.sfInputs:
                df, max_value = read_sf(sf_input)
                if max_value is not None:
                    dbms_audit_dict[sf_input.dataFrameName] = max_value
                dataframe_dict[sf_input.dataFrameName] = df

        if job.sfInputs:
            for sf_input in job.sfInputs:
                if sf_input.dataFrameName in dbms_audit_dict:
                    sfdb_audit_write(
                        sf_input.sfURL,
                        sf_input.sfUser,
                        sf_input.sfPassword,
                        sf_input.sfAccount,
                        sf_input.sfDatabase,
                        sf_input.sfSchema,
                        sf_input.sfRole,
                        sf_input.sfdbIncremental.auditTableName,
                        sf_input.sfdbIncremental.ingestionID,
                        dbms_audit_dict[sf_input.dataFrameName],
                        dataframe_dict[sf_input.dataFrameName]
                    )

        if job.fileOutputs:
            for file_output in job.fileOutputs:
                file_writer(
                    dataframe_dict[file_output.dataFrameName],
                    file_output.format,
                    file_output.fileOutPath,
                    file_output.mode,
                    file_output.rollup,
                    file_output.header,
                    file_output.snakeCaseHeader,
                    file_output.delimiter,
                    file_output.numPartitions,
                    file_output.partitionBy,
                    file_output.addColumns,
                    file_output.selectCol
                )

        if job.dbmsInputs:
            for dbms_input in job.dbmsInputs:
                if dbms_input.dataFrameName in dbms_audit_dict:
                    db_audit_write(
                        dbms_input.dbIncremental.auditTablePath,
                        dbms_input.dbIncremental.ingestionID,
                        dbms_audit_dict[dbms_input.dataFrameName],
                        dataframe_dict[dbms_input.dataFrameName]
                    )

        if job.dbmsOutputs:
            for dbms_output in job.dbmsOutputs:
                dbms_writer(
                    dataframe_dict[dbms_output.dataFrameName],
                    dbms_output.url,
                    dbms_output.driver,
                    dbms_output.user,
                    dbms_output.password,
                    dbms_output.dbtable,
                    dbms_output.mode
                )

        if job.mongoOutputs:
            for mongo_output in job.mongoOutputs:
                mongo_writer(
                    dataframe_dict[mongo_output.dataFrameName],
                    mongo_output.uri,
                    mongo_output.database,
                    mongo_output.collection,
                    mongo_output.mode,
                    mongo_output.selectCol
                )

        if job.sfOutputs:
            for sf_output in job.sfOutputs:
                sf_writer(
                    dataframe_dict[sf_output.dataFrameName],
                    sf_output.sfURL,
                    sf_output.sfAccount,
                    sf_output.sfUser,
                    sf_output.sfPassword,
                    sf_output.sfDatabase,
                    sf_output.sfSchema,
                    sf_output.sfRole,
                    sf_output.dbtable,
                    sf_output.mode
                )

        pretty_print( " Job Completed  -- " + str(job.name) )


def read_file(file_input):
    from consume.read_file import file_reader
    return file_reader(
        file_input.fileInPath,
        file_input.fileName,
        file_input.unzipFilePath,
        file_input.dataFormat,
        file_input.rollup,
        file_input.snakeCaseHeader,
        file_input.enforceSchemaFile,
        file_input.numPartitions,
        file_input.selectCol
    )

def read_table(dbms_input):
    from consume.read_table import table_reader
    return table_reader(
        dbms_input.driver,
        dbms_input.url,
        dbms_input.user,
        dbms_input.password,
        dbms_input.dbtable,
        dbms_input.dbIncremental,
        dbms_input.numPartitions,
        dbms_input.partitionColumn,
        dbms_input.lowerBound,
        dbms_input.upperBound,
        dbms_input.queryTimeout,
        dbms_input.fetchsize,
        dbms_input.pushDownPredicate
    )

def read_mongo(mongo_input):
    from consume.read_mongo import mongo_reader
    return mongo_reader(
        mongo_input.uri,
        mongo_input.database,
        mongo_input.collection,
        mongo_input.selectCol,
        mongo_input.filter,
        mongo_input.incr_col,
        mongo_input.incr_load_flg,
        mongo_input.audit_table_name
    )

def read_sf(sf_input):
    from consume.read_sf import sf_reader
    return sf_reader(
        sf_input.sfURL,
        sf_input.sfAccount,
        sf_input.sfUser,
        sf_input.sfPassword,
        sf_input.sfDatabase,
        sf_input.sfSchema,
        sf_input.sfRole,
        sf_input.query,
        sf_input.selectCol,
        sf_input.filter,
        sf_input.sfdbIncremental
    )