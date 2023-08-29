#from azure.eventhub.aio import EventHubProducerClient, EventData
from azure.eventhub import EventHubProducerClient,EventData
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import to_json
from pyspark.sql.types import *
import time


if __name__ =='__main__':
    connection_str = 'Endpoint=sb://eventhub-asa-demo.servicebus.windows.net/;SharedAccessKeyName=londoncrimes;SharedAccessKey=Ok5vbEGoTyJhghlPkSman9q36JEpZIvRkh7806VcEcI='
    consumer_group = '<< CONSUMER GROUP >>'
    eventhub_name = 'londoncrimes-eh'
    producer_client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
    event_data_batch = producer_client.create_batch()
    spark=SparkSession.builder.appName("Event Hub Message Publish").getOrCreate()
    schema = StructType([StructField("lsoa_code", StringType(), True), \
                         StructField("borough", StringType(), True), \
                         StructField("major_category", StringType(), True), \
                         StructField("minor_category", StringType(), True), \
                         StructField("value", StringType(), True), \
                         StructField("year", StringType(), True), \
                         StructField("month", StringType(), True)])

    df=spark.read.format("csv").schema(schema).load("C:\\Users\jvisvanathan\PycharmProjects\PycharmExample\SparkStreaming\\datasets\\dropLocation")
    event_exception=False
    for i in df.collect():
        message={"lsoa_code":i.lsoa_code,"borough":i.borough,"major_category":i.major_category, \
        "minor_category":i.minor_category \
        ,"value":i.value \
        ,"year":i.year \
        ,"month":i.month}
        print("Message Type: {0}".format(type(message)))
        print(message)
        try:
            event_data_batch.add(EventData(message))
        except ValueError:
            producer_client.send_batch(event_data_batch)
            time.sleep(2)
            event_data_batch = producer_client.create_batch()
        except Exception as e:
            event_exception=True

        if not event_exception:
            producer_client.send_batch(event_data_batch)



