from pyspark.sql.functions import *
from dependencies.datetime_utilities import get_timestamp
import datetime
from pyspark.sql import functions as F, DataFrame
from dependencies.pretty_print import pretty_print


def sfdb_audit_query(
        sfUrl,
        sfAccount,
        sfUser,
        sfPassword,
        sfDatabase,
        sfSchema,
        sfRole,
        query,
        selectCol,
        sfdbIncremental

):
    sfOptions = dict(sfUrl=sfUrl,
                     sfUser=sfUser,
                     sfPassword=sfPassword,
                     sfAccount=sfAccount,
                     sfDatabase=sfDatabase,
                     sfSchema=sfSchema,
                     sfRole=sfRole
                     )
    from dependencies.spark import spark_session
    pretty_print(" Initiating Audit Read")

    audit_table_df = spark_session.read.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable",
                            sfdbIncremental.auditTableName).load().filter(F.col("INGESTION_ID") == sfdbIncremental.ingestionID)

    min_value = str(audit_table_df.orderBy(F.col("INGESTION_TIMESTAMP"), ascending=False).select("AUDIT_ID").first()[0])

    pretty_print("Min Value : " + min_value)
    max_query = sfdbIncremental.maxQuery.replace("{minValue}", min_value)
    pretty_print("Max Query : " + max_query)

    max_value = \
    spark_session.read.format("net.snowflake.spark.snowflake").options(**sfOptions).option("query", max_query).load().first()[0]

    pretty_print("Max value : " + str(max_value))

    query = query.replace("{minValue}", min_value).replace("{maxValue}", str(max_value))

    pretty_print("Final query : " + query)
    return query, max_value


def sfdb_audit_write(sfUrl,
                   sfUser,
                   sfPassword,
                   sfAccount,
                   sfDatabase,
                   sfSchema,
                   sfRole,
                   audit_table_name,
                   ingestion_id,
                   max_value,
                   dataframe: DataFrame
                   ):
    sfOptions = dict(sfUrl=sfUrl,
                     sfUser=sfUser,
                     sfPassword=sfPassword,
                     sfAccount=sfAccount,
                     sfDatabase=sfDatabase,
                     sfSchema=sfSchema,
                     sfRole=sfRole
                     )
    from dependencies.spark import spark_session

    audit_table_df = spark_session.read.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable",
                    audit_table_name).load().filter(F.col("INGESTION_ID") == ingestion_id).orderBy(F.col("INGESTION_TIMESTAMP"), ascending=False).limit(1)

    audit_table_df = audit_table_df.withColumn("INGESTION_TIMESTAMP", F.lit(datetime.datetime.now().isoformat()))

    if bool(dataframe.head(1)):
        audit_table_df = audit_table_df.withColumn("AUDIT_ID", F.lit(str(max_value)))

    audit_table_df.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable",audit_table_name).mode("Append").save()

    pretty_print("Audit write successful ")
    pass
