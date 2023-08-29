# -*- coding: utf-8 -*-
"""
Created on 21/04/20 10:20 PM

@author : Nirav Langaliya
"""

# File Name : SparkTestScript.py

# Enter feature description here

# Enter steps here

##pyspark --jars databricks_delta.jar

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("jdbc data sources") \
    .getOrCreate()

df = spark.read.format("delta").load("/Users/nlangaliya/Downloads/IngestionFramework/OutputData/delta")

df.show()