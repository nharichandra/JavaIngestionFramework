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
#   .option("subscribe", "gdntyg54-test_topic") \
    kafkaDf = spark.read.format("kafka") \
                    .option("kafka.bootstrap.servers",
                    "SASL_SSL://moped-01.srvs.cloudkafka.com:9094,SASL_SSL://moped-02.srvs.cloudkafka.com:9094,SASL_SSL://moped-03.srvs.cloudkafka.com:9094") \
                    .option("assign", """{"gdntyg54-test_topic":[1,3,4]}""") \
                    .option("startingOffsets","""{"gdntyg54-test_topic":{"1":13236,"3":14581,"4":26598}}""") \
                    .option("endingOffsets","""{"gdntyg54-test_topic":{"1":13272,"3":14654,"4":26775}}""") \
                    .option("kafka.security.protocol", "SASL_SSL") \
                    .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
                    .option("kafka.sasl.jaas.config", \
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username='gdntyg54' password='Tp7JmiP3NPN90MNucuhtRNquipdNpF2k';").load()
    print(kafkaDf.printSchema())
    df1=kafkaDf.withColumn("key",kafkaDf["key"].cast(StringType())) \
               .withColumn("value",kafkaDf["value"].cast(StringType()))

    df1.createOrReplaceTempView("kafka_ip_tbl")
    df_out=spark.sql("select key, SUM(CAST(value as int)) as value from kafka_ip_tbl group by key order by value desc")

    print(df_out.show())






