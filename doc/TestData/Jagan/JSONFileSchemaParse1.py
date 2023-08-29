from pyspark.sql import SparkSession
import json
import pyspark.sql.types as st
import os


if __name__ == '__main__':
    spark=SparkSession.builder.master("local").appName("schema test").getOrCreate()


    with open("/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/resources/london_crimes.json") as file_obj:
        schema_raw=file_obj.read()
        print("Type of Schema: {0}".format(type(schema_raw)))

    schema_dict=json.loads(schema_raw)
# Parse JSON string into python dictionary
#    schema_dict = json.loads(schema_json_str)
# Create StructType from python dictionary
    schema = st.StructType.fromJson(schema_dict)

    crimes_df=spark.read.format("csv").schema(schema).load("/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/dropLocation/xab.csv")


#    user_df = spark.createDataFrame(data, schema)
    print(crimes_df.show())