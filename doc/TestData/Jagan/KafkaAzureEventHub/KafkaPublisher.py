from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *


if __name__=='__main__':
    spark=SparkSession.builder.master("local").appName("StreamAggSQL").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions",2)
#    spark.conf.set("java.security.auth.login.config","/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/conf/jaas.conf")
    spark.sparkContext.setLogLevel("INFO")


#    kafkaClient=pykafka.KafkaClient(hosts="wn0-first.cppnfcf2hhguzcmuway5ojjrwb.bx.internal.cloudapp.net:9092")

    schema = StructType([StructField("lsoa_code", StringType(), True), \
                         StructField("borough", StringType(), True), \
                         StructField("major_category", StringType(), True), \
                         StructField("minor_category", StringType(), True), \
                         StructField("value", StringType(), True), \
                         StructField("year", StringType(), True), \
                         StructField("month", StringType(), True)])

    def addTimeStamp():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    udf_addTimeStamp=udf(addTimeStamp,StringType())
#    with open("/mnt/c/users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/resources","r") as file_obj:
#        file_schema=file_obj.readlines()
#    print(file_schema)
    crimesDF=spark.readStream.format("csv") \
                                .option("maxFilesPerTrigger",1) \
                                .schema(schema) \
                                .load("/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/dropPath")

    crimesDFWithTimeStamp=crimesDF.withColumn("timestamp",udf_addTimeStamp()) \
                                    .withColumn("value",crimesDF["value"].cast(IntegerType())) \
                                    .withColumn("year",crimesDF["year"].cast(IntegerType())) \
                                    .withColumn("month",crimesDF["month"].cast(IntegerType()))


    crimesDF.createOrReplaceTempView("LondonCrimesTbl")

    convictionsPerCategory=spark.sql("select /*+ REPARTITION(8) */ major_category as major_category, SUM(value) as Conviction_Count from LondonCrimesTbl group by major_category ")

#    query=convictionsPerCategory.selectExpr("CAST(major_category AS STRING) as key", \
#                                            "to_json(struct(*)) as value") \

#   For Kafka Outputs below are the supported modes.
#   Append mode is supported only when no Aggregation is performed (DEFAULT)
#   Update mode is supported for Aggregation queries where no order by is used
#   Complete mode is used for Aggregation Queries with/Without Order By

#    query = convictionsPerCategory.selectExpr("CAST(major_category AS STRING) as key", \
#        "CAST(Conviction_Count as STRING) as value") \
#   APPEND MODE
#    query=crimesDF.selectExpr("CAST(major_category AS STRING)", "to_json(struct(*)) as value") \
#   crimesDFWithTimeStamp.selectExpr("CAST(major_category AS STRING)", "to_json(struct(*)) as value") \
    query = crimesDF.selectExpr("CAST(major_category AS STRING)", "to_json(struct(*)) as value") \
        .writeStream \
        .format("kafka") \
        .outputMode("update") \
        .option("badRecordsPath","/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/KafkaPublisher") \
        .option("kafka.bootstrap.servers","moped-01.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-03.srvs.cloudkafka.com:9094") \
        .option("topic","gdntyg54-test_topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='gdntyg54' password='Tp7JmiP3NPN90MNucuhtRNquipdNpF2k';") \
        .trigger(processingTime="2 seconds") \
        .option("checkpointLocation","/mnt/c/Users/jvisvanathan/PycharmProjects/PycharmExample/SparkStreaming/datasets/CheckPointLocation/KafkaPublisher/") \
        .start() \
        .awaitTermination()
#


# Password=Tp7JmiP3NPN90MNucuhtRNquipdNpF2k
# API_KEY=24a69076-e1c6-4e6d-b7e3-98f19d6e5444
#        .option("kafka.sasl.enabled","true") \




#    query=convictionsPerCategory.writeStream.format("memory") \
#                                    .outputMode("complete") \
#                                .queryName("ConvictionsPerCategory") \
#                               .option("truncate","false") \
#                                .trigger(processingTime="2 seconds") \
#                                .start() \
#                                .awaitTermination()#

#    spark.sql("select * from ConvictionsPerCategory").show()
#"""
#                                 .awaitTermination()


#        .option("sasl_plain_username","gdntyg54") \
#        .option("sasl_plain_password","Tp7JmiP3NPN90MNucuhtRNquipdNpF2k") \
#        .option("sasl_mechanism","SCRAM-SHA-256") \