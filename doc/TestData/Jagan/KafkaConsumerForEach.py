from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
import os,uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


def writeDatatoFileSys(df,epoch_id):

#    df.write.format("jdbc") \
#        .option("driver","org.postgresql.Driver") \
#        .option("url", "jdbc:postgresql://drona.db.elephantsql.com:5432/inchcydi?") \
#        .option("dbtable", "public.london_crimes1") \
#        .option("user", "inchcydi") \
#        .option("password", "2UDDOpRKvdq-shLyjMzkp3tZXYTTADk9") \
#        .save()
    op_location="/mnt/c/users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/OutputDir/"+str(epoch_id)
    df.repartition(10).write.format("csv").mode("append").save(op_location)
#        .option("mode","append") \


def writeDatatoAzureFileSystem(df,epoc_id):
    try:
        print("Trying to write data to Azure Blob Storage")
    except Exception as e:
            print("Exception: {0}".format(e))

#   SASL_SSL://
    df.write.format("jdbc")
if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("Kafka Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    kafkaDf = spark.readStream.format("kafka") \
                            .option("kafka.bootstrap.servers","moped-01.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-03.srvs.cloudkafka.com:9094") \
                            .option("subscribe", "gdntyg54-test_topic") \
                            .option("startingOffsets","latest") \
                            .option("kafka.security.protocol", "SASL_SSL") \
                            .option("kafka.sasl.mechanism","SCRAM-SHA-256") \
                            .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='gdntyg54' password='Tp7JmiP3NPN90MNucuhtRNquipdNpF2k';").load()

    schema = StructType([StructField("lsoa_code", StringType(), True), \
                         StructField("borough", StringType(), True), \
                         StructField("major_category", StringType(), True), \
                         StructField("minor_category", StringType(), True), \
                         StructField("value", StringType(), True), \
                         StructField("year", StringType(), True), \
                         StructField("month", StringType(), True)])
#    df = kafkaDf.selectExpr("CAST(key as STRING) as key", "CAST(value AS STRING)")
#   Parsing JSON Data present in Kafka Broker"from_json(\"(CAST(value) as STRING\"),schema) as value")
#    df = kafkaDf.selectExpr("CAST(key as STRING) as key","from_json(CAST(value AS STRING),schema) as value")
    df1=kafkaDf.withColumn("key",kafkaDf["key"].cast(StringType())) \
               .withColumn("value",from_json(kafkaDf["value"].cast(StringType()),schema))
    print(df1.printSchema())
    query=df1.select("value.lsoa_code","value.borough","value.major_category","value.minor_category","value.value","value.year","value.month") \
            .writeStream \
            .trigger(processingTime="2 seconds") \
            .foreachBatch(writeDatatoFileSys) \
            .option("checkpointLocation","/mnt/c/users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/CheckPointLocation/KafkaConsumertoSQLDB") \
            .start().awaitTermination()


