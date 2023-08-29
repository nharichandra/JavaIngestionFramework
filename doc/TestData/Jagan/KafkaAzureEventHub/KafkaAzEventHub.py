"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4,com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.15 KafkaAzEventHub.py
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,to_json, struct, window, col
from pyspark import  SparkContext as sc
from pyspark.sql.window import *

import os,uuid


def writeDatatoSQLDB(df,epoch_id):

#   While trying to write to Different JDBC Destination
#   1) overwrite -> Truncate Table and overwrite data in table
#   2) append -> use option("allowAppend", True), mode("append")
#   3) Ignore -> If table is not empty, it doesn't write. If table is empty it writes data to table
#   4) ErrorIfExists -> If data already exist, throw exception

    df.repartition(10).write.format("jdbc") \
        .option("driver","org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://drona.db.elephantsql.com:5432/inchcydi?") \
        .option("user", "inchcydi") \
        .option("password", "2UDDOpRKvdq-shLyjMzkp3tZXYTTADk9") \
        .option("dbtable", "public.londoncrimesagg") \
        .mode("append") \
        .option("allowAppend", True) \
        .save()



def writeDatatoFileSys(df,epoch_id):

    op_location="/mnt/c/users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/OutputDir/"+str(epoch_id)
    df.repartition(10).write.format("csv").mode("append").save(op_location)
#        .option("mode","append") \





if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("Kafka Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    spark.conf.set("spark.sql.shuffle.partitions",2)
#
    ehConf = {}
    connectionString="Endpoint=sb://eventhub-asa-demo.servicebus.windows.net/londoncrimes-eh;EntityPath=londoncrimes-eh;SharedAccessKeyName=londoncrimes-access-policy;SharedAccessKey=6vzYHFNY3iPrvFVyiy7DnmO0rSql3zCpQr/aN8JrXpY="
    ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
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
                         StructField("value", IntegerType(), True), \
                         StructField("year", IntegerType(), True), \
                         StructField("month", IntegerType(), True),\
                         StructField("timestamp",TimestampType(),True)])

    df1 = kafkaDf.withColumn("partitionKey", kafkaDf["key"].cast(StringType())) \
        .withColumn("body", kafkaDf["value"])

    query = df1.selectExpr("CAST(partitionKey AS STRING) as partitionKey", \
                           "CAST(body as STRING) as body")

    query = df1.selectExpr("CAST(partitionKey AS STRING) as partitionKey", "CAST(body as STRING) as body") \
            .writeStream \
           .format("eventhubs") \
            .options("**ehconf)") \
            .outputMode("append")\
            .option("checkpointLocation","/mnt/c/users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/CheckPointLocation/KafkaConsumerAzEventHub") \
            .start() \
            .awaitTermination()

"""
APPEND MODE:
    df1=kafkaDf.withColumn("partitionKey",kafkaDf["key"].cast(StringType())) \
                        .withColumn("body",kafkaDf["value"])

    query=df1.selectExpr("CAST(partitionKey AS STRING) as partitionKey", \
                                            "CAST(body as STRING) as body") \



COMPLETE MODE
    df= kafkaDf.selectExpr("CAST(key AS STRING) as key","CAST(value AS STRING) as value")\
                  .select(from_json("value",schema).alias("value")) \
                  .select("value.*")
    print(df.printSchema())
    df.createOrReplaceTempView("crimesTbl")
    CrimesCountByMajorCategory=spark.sql("select /*+ REPARTITION(8) */ major_category as major_category, SUM(value) as Conviction_Count from crimesTbl group by major_category order by Conviction_Count DESC")


#   UPDATE MODE
#   df1=kafkaDf.selectExpr("CAST(key as STRING) as key","from_json(kafkaDf['value'].cast(StringType()),schema) as value")
    df1 = kafkaDf.selectExpr("CAST(key AS STRING) as key","CAST(value AS STRING) as value")\
                  .select(from_json("value",schema).alias("value")) \
                  .select("value.*")

    print(df1.printSchema())
    convictionCounts=df1 \
                    .withWatermark("timestamp", "5 seconds") \
                    .groupBy(window(df1["timestamp"], "5 seconds", "2 seconds"),df1["major_category"]) \
                    .agg({"value":"sum"}) \
                    .withColumnRenamed("sum(value)","AggregatedConvictionCounts")
    print(convictionCounts.printSchema())

    convictionCountsJSON=convictionCounts.selectExpr("CAST(window as STRING) as partitionKey",\
                            "to_json(struct(*)) as body")

    print(convictionCountsJSON.printSchema())
    
    #    Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
    windowedCounts=df\
                    .withWatermark(df["timestamp"],"10 seconds") \
                    .groupBy(window(df["timestamp"], "30 seconds","18 seconds"))\
                .agg({"value":"sum"}) \
                .withColumnRenamed("sum(value)","convictions") \
                .orderBy("convictions",ascending=False)

"""