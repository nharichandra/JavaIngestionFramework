from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json

if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("Kafka Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    kafkaDf = spark.readStream.format("kafka") \
                            .option("kafka.bootstrap.servers","SASL_SSL://moped-01.srvs.cloudkafka.com:9094,SASL_SSL://moped-02.srvs.cloudkafka.com:9094,SASL_SSL://moped-03.srvs.cloudkafka.com:9094") \
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

#    print(df1.show(10,truncate=False))
#    query = df.writeStream.format("console").option("truncate","false").start().awaitTermination()

#   Writing Response back to another Kafka Broker
#    query = kafkaDf.selectExpr("CAST(key as STRING) as key", "CAST(value AS STRING)") \
    query = df1.select(df1["key"].cast(StringType()),df1["value"].cast(StringType()))\
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
