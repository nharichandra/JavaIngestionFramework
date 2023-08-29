# -*- coding: utf-8 -*-
"""
Created on 04/12/2020 2:01 PM

@author : Nirav Langaliya
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import configparser


def spark_conf(property_file):
    prop = configparser.ConfigParser()
    prop.read(property_file)
    """ tbd """

spark_session= SparkSession.builder.appName('Data-Ingestion').config("spark.master", "local").getOrCreate()
         #.config()\   # .enableHiveSupport()\

# spark_context = spark_session.sparkContext
# spark_conf = spark_session.conf

spark = SparkSession.builder.master('local').getOrCreate()