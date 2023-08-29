from pyspark.sql.functions import *
import datetime
from dependencies.spark import spark

def incremental_map(uri,
        db,
        SRC_table,
        selectCols,
        incr_col,
        incr_load_flg,
        audit_table_name):
    # Checking if we have to perform the Incremental load/not.
    # if incr_load_flg=='Y' then we will extract the max incremental value from audit table.the we are converting df to list.
    if incr_load_flg == 'Y':

        df_audit = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",uri).option("multiline", "true").option(
            "database", db).option("collection", audit_table_name).load()
        max_Ingestion_time = df_audit.withColumn("INGESTION_TIMESTAMP", col("INGESTION_TIMESTAMP")).agg(
            max("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP")).rdd.flatMap(lambda x: x).collect()
        auditDetails = df_audit.select("AUDIT_ID").filter(
            df_audit.INGESTION_TIMESTAMP == max_Ingestion_time[0]).rdd.flatMap(lambda x: x).collect()

        # once we extracted value from Audit table we will that value as a filter into main data frame.

        df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",uri).option("multiline", "true").option("database",
                                                                                                         db).option(
            "collection", SRC_table).load()
        df = df.select(selectCols).filter(df[incr_col] > auditDetails[0])

        # Below one is uesed to get the max value from DF and will insert into audit table for next run refernace.

        Write_maxValue = df.withColumn(incr_col, col(incr_col)).agg(max(incr_col).alias(incr_col)).rdd.flatMap(
            lambda x: x).collect()

        df_insert_audit = spark.createDataFrame([{"INGESTION_ID": SRC_table, "INC_COL": incr_col,
                                                  "AUDIT_ID": Write_maxValue[0],
                                                  "INGESTION_DESC": "Incremental Data load/ Delta load",
                                                  "INGESTION_TIMESTAMP": datetime.datetime.now().isoformat()}])

        # df_insert_audit.show()

        df_insert_audit.write.format("com.mongodb.spark.sql.DefaultSource").option("uri",uri).mode("append").option("inferSchema",
                                                                                                  "true").option(
            "database", "HR").option("collection", "Audit").save()

    # If incr_load not 'Y' then we will extract whole data to process.
    else:
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",uri).option("multiline", "true").option("database",
                                                                                                         db).option(
            "collection", SRC_table).load()
        df = df.select(selectCols)

        # Below one is uesed to get the max value from DF and will insert into audit table for next run refernace.
        Write_maxValue = df.withColumn(incr_col, col(incr_col)).agg(max(incr_col).alias(incr_col)).rdd.flatMap(
            lambda x: x).collect()

        df_insert_audit = spark.createDataFrame([{"INGESTION_ID": SRC_table, "INC_COL": incr_col,
                                                  "AUDIT_ID": Write_maxValue[0],
                                                  "INGESTION_DESC": "Full data Load/Initial Load",
                                                  "INGESTION_TIMESTAMP": datetime.datetime.now().isoformat()}])

        # df_insert_audit.show()

        df_insert_audit.write.format("com.mongodb.spark.sql.DefaultSource").option("uri",uri).mode("append").option("inferSchema",
                                                                                                  "true").option(
            "database", "HR").option("collection", "Audit").save()

    return df