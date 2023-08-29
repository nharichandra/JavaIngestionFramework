from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
global isStreaming
global streamQuery

if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("Kafka Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions",2)

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
    streamingFile="/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/conf/isStreaming.txt"
    isStreaming=True
    streamQuery='StreamingQuery1'
    print("Active Streams: {0}".format(spark.streams.active))
#    spark.streams.resetTerminated()
#    spark.streams.awaitAnyTermination()

    while isStreaming:
        try:
            with open(streamingFile,'r') as file_obj:
                streaming=file_obj.read()
        except FileNotFoundError as e:
            print("Stopping Stream: {0}".format(e.strerror))
            isStreaming=False
        finally:
            if isStreaming:
                kafkaDf = spark.readStream.format("kafka") \
                        .option("kafka.bootstrap.servers",
                        "SASL_SSL://moped-01.srvs.cloudkafka.com:9094,SASL_SSL://moped-02.srvs.cloudkafka.com:9094,SASL_SSL://moped-03.srvs.cloudkafka.com:9094") \
                        .option("subscribe", "gdntyg54-test_topic") \
                        .option("startingOffsets", "latest") \
                        .option("kafka.security.protocol", "SASL_SSL") \
                        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
                        .option("kafka.sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username='gdntyg54' password='Tp7JmiP3NPN90MNucuhtRNquipdNpF2k';").load()
                print(kafkaDf.printSchema())
                df1=kafkaDf.withColumn("key",kafkaDf["key"].cast(StringType())) \
                            .withColumn("value",kafkaDf["value"].cast(StringType()))

                df1.createOrReplaceTempView("kafka_ip_tbl")
                df_out=spark.sql("select key, SUM(CAST(value as int)) as value from kafka_ip_tbl group by key order by value desc")
                streamDf=df_out.select(df_out["key"].cast(StringType()),df_out["value"].cast(StringType())) \
                .writeStream \
                .format("console")\
                .outputMode("complete") \
                .trigger(processingTime="2 seconds") \
                .start()

            else:
                if streamDf.isActive:
                    streamDf.processAllAvailable()
#                assert(spark.streams.active.isEmpty)
                print("Stopping Streaming Query: {0}- {1}".format(streamDf.id,streamDf.name))
                print("Streaming Query Status: {0}".format(streamDf.status))
#                streamDf.awaitTermination(5000)
                streamDf.stop()
                print("Streaming Stopped")
                file_obj.close()





#    print(df1.show(10,truncate=False))
#    query = df.writeStream.format("console").option("truncate","false").start().awaitTermination()

#   Writing Response back to another Kafka Broker
#    query = kafkaDf.selectExpr("CAST(key as STRING) as key", "CAST(value AS STRING)") \
'''  
    query1 = df1.select(df1["key"].cast(StringType()),df1["value"].cast(StringType()))\
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers",
                    "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094") \
            .option("topic", "qo4q1apb-test_topic") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
            .option("kafka.sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username='qo4q1apb' password='6xEvVxnr7lrxDrbrioX9D9mtvOY2gK1M';") \
            .trigger(processingTime="2 seconds") \
            .option("checkpointLocation", \
                    "/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/CheckPointLocation/KafkaConsumer-Publisher/") \
            .start() \
            .awaitTermination()
'''
