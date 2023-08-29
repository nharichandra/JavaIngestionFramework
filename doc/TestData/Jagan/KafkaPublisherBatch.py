from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from datetime import datetime
from pyspark.sql.functions import *
import os


def writetoKafka(df):
    df.createOrReplaceTempView("london_crimes")
    df1=spark.sql("select major_category, SUM(CAST(value as int)) as conviction_count from london_crimes group by major_category")
    df1.selectExpr("CAST(major_category AS STRING) as key",\
                "CAST(conviction_count as STRING) as value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "moped-01.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-03.srvs.cloudkafka.com:9094") \
        .option("topic", "gdntyg54-test_topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.sasl.jaas.config", \
                "org.apache.kafka.common.security.scram.ScramLoginModule required username='gdntyg54' password='Tp7JmiP3NPN90MNucuhtRNquipdNpF2k';") \
        .save()

def createDF(inputFilePath,ip_schema):
        print("Input Schema: {0}".format(ip_schema))
        df=spark.read.format("csv").schema(ip_schema).load(inputFilePath)
        print(df.show())
        writetoKafka(df)



if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("StreamAggSQL").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions",2)
    spark.sparkContext.setLogLevel("INFO")

    ip_schema = StructType([StructField("lsoa_code", StringType(), True), \
                         StructField("borough", StringType(), True), \
                         StructField("major_category", StringType(), True), \
                         StructField("minor_category", StringType(), True), \
                         StructField("value", StringType(), True), \
                         StructField("year", StringType(), True), \
                         StructField("month", StringType(), True)])

    def addTimeStamp():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    udf_addTimeStamp=udf(addTimeStamp,StringType())

    file_path="/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/dropLocation/"

    if  os.path.isdir(file_path):
        for file in os.listdir(file_path):
            filepath=file_path+file
            createDF(filepath,ip_schema)
    else:
        createDF(file_path,ip_schema)
