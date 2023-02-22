package com.nisum.dataingestionframework.dependencies;

import com.nisum.dataingestionframework.model.DataFormat;
import com.nisum.dataingestionframework.model.Job;
import com.nisum.dataingestionframework.model.SslParameter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeoutException;

/* @author Hari Chandra Prasad Nimmagadda */

public class SchemaUtilities implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SchemaUtilities.class);

    String dataFormat;
    Dataset<Job> dataFrame;
    Map<String, Dataset<Row>> d1 = new HashMap<String, Dataset<Row>>();
    Properties property;

    public Map getOptions(boolean header, String delimiter, boolean inferschema, boolean multiline, String rootag, String rowtag) {

        Map options = new HashMap();
        Field field = null;

        if(header) {
            try {
                field = DataFormat.class.getDeclaredField("header");
                options.put(field.getName(), header);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        if(!delimiter.isEmpty()) {
            try {
                field = DataFormat.class.getDeclaredField("delimiter");
                options.put(field.getName(), delimiter);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        if(inferschema) {
            try {
                field = DataFormat.class.getDeclaredField("inferSchema");
                options.put(field.getName(), inferschema);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        if(multiline) {
            try {
                field = DataFormat.class.getDeclaredField("multiLine");
                options.put(field.getName(), multiline);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        if(!rootag.isEmpty()) {
            try {
                field = DataFormat.class.getDeclaredField("roottag");
                options.put(field.getName(), rootag);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        if(!rowtag.isEmpty()) {
            try {
                field = DataFormat.class.getDeclaredField("rowtag");
                options.put(field.getName(), rowtag);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }

        return options;
    }

    public Map getOptions(boolean header, String delimiter) {

        Map options = new HashMap();
        Field field = null;

        if(header) {
            options.put("header", String.valueOf(header));
            /*try {
                field = DataFormat.class.getDeclaredField("header");
                options.put(field.getName(), header);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }*/
        }

        if(!delimiter.isEmpty()) {

            options.put("delimiter", delimiter);
            /*try {
                field = DataFormat.class.getDeclaredField("delimiter");
                options.put(field.getName(), delimiter);

            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }*/
        }

        return options;
    }


    public DataFrameReader getFileReader(String schemaFile, Map options) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("DataIngestionApp").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

         DataFrameReader dataFrameReader = sparkSession.read();

        options.forEach((k, v) -> logger.info((k + ":" + v)));

        options.forEach((k, v) -> {
            dataFrameReader.option(k.toString(), v.toString());
        });


        // Schema File
        /*Optional<String> schemaFileName = Optional.of(schemaFile);

        schemaFileName.ifPresent(s ->
                dataFrameReader.schema(StructType.fromJson(s).json())
        // dataFrameReader.schema(StructType.fromJson(schemaFile).toString());
        );
*/

        return dataFrameReader;

    }

    public Dataset<Row> getFileWriter(Dataset<Row> writer, Map options) {

        options.forEach((k, v) -> logger.info((k + ":" + v)));

        options.forEach((k, v) -> {
            writer.write().option(k.toString(), v.toString());
        });

        return writer;
    }

    public StructType getCsvSchema() {

    StructType schema = new StructType(new StructField[]{

           // EMP SNO|EMP NAME|EMP AGE|EMP EMAIL|EMP SALARY

            new StructField("EMP SNO", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("EMP NAME", DataTypes.StringType, false, Metadata.empty()),
            new StructField("EMP AGE", DataTypes.StringType, false, Metadata.empty()),
            new StructField("EMP EMAIL", DataTypes.StringType, false, Metadata.empty()),
            new StructField("EMP SALARY", DataTypes.StringType, false, Metadata.empty())});

        return schema;

    }

    public Map getJdbcOptions(String url, String user, String password, String driver, String dbtable, int numPartitions,
                              String partitionColumn, String lowerBound, String upperBound, int queryTimeout,
                              int fetchsize, boolean predicatePushdown) {

        Map options = new HashMap();

        Optional.ofNullable(url).ifPresent(s -> {
            options.put("url", s);
        });

        Optional.ofNullable(driver).ifPresent(s -> {
            options.put("driver", s);
        });

        Optional.ofNullable(user).ifPresent(s -> {
            options.put("user", s);
        });

        Optional.ofNullable(password).ifPresent(s -> {
            options.put("password", s);
        });

        Optional.ofNullable(dbtable).ifPresent(s -> {
            options.put("dbtable", s);
        });

        Optional.ofNullable(numPartitions).ifPresent(integer -> {
            options.put("numPartitions", numPartitions);
        });

        Optional.ofNullable(partitionColumn).ifPresent(integer -> {
            options.put("partitionColumn", partitionColumn);
        });

        Optional.ofNullable(lowerBound).ifPresent(integer -> {
            options.put("lowerBound", lowerBound);
        });

        Optional.ofNullable(upperBound).ifPresent(integer -> {
            options.put("upperBound", upperBound);
        });

        Optional.ofNullable(queryTimeout).ifPresent(integer -> {
            options.put("queryTimeout", queryTimeout);
        });

        Optional.ofNullable(fetchsize).ifPresent(integer -> {
            options.put("fetchsize", fetchsize);
        });

        Optional.ofNullable(predicatePushdown).ifPresent(integer -> {
            options.put("predicatePushdown", predicatePushdown);
        });

        Optional.ofNullable(numPartitions).ifPresent(integer -> {
            options.put("numPartitions", numPartitions);
        });

        logger.info(" DBMSInput options ........ " + options);

        return options;

    }

    public Map getKafkaOptions(String servers, List<String> topics,String subscribe, String consumerGroup, SslParameter ssl,String startingOffsets,
                               boolean autoCommit,int autoCommitInterval,boolean failOnDataLoss) {

        Map options = new HashMap();


        Optional.ofNullable(servers).ifPresent(s -> {
            options.put("kafka.bootstrap.servers", s);
        });

        Optional.ofNullable(topics).ifPresent(s -> {
            options.put("topics", s);
        });

        Optional.ofNullable(subscribe).ifPresent(s -> {
            options.put("subscribe", s);
        });

        Optional.ofNullable(consumerGroup).ifPresent(s -> {
            options.put("consumerGroup", s);
        });

        Optional.ofNullable(ssl).ifPresent(s -> {
            options.put("ssl", s);
        });

        Optional.ofNullable(startingOffsets).ifPresent(s -> {
            options.put("startingOffsets", s);
        });

        Optional.ofNullable(autoCommit).ifPresent(integer -> {
            options.put("autoCommit", autoCommit);
        });

        Optional.ofNullable(autoCommitInterval).ifPresent(integer -> {
            options.put("autoCommitInterval", autoCommitInterval);
        });

        Optional.ofNullable(failOnDataLoss).ifPresent(integer -> {
            options.put("failOnDataLoss", failOnDataLoss);
        });

        logger.info(" DBMSInput options ........ " + options);

        return options;

    }

    public Map getKafkaOutputOptions(String servers, List<String> topics,String key, String outputMode, SslParameter ssl, String trigger, String outputDataFormat, String checkPointLocation) {

        Map options = new HashMap();


        Optional.ofNullable(servers).ifPresent(s -> {
            options.put("kafka.bootstrap.servers", s);
        });

        Optional.ofNullable(topics).ifPresent(s -> {
            options.put("topics", s);
        });

        Optional.ofNullable(key).ifPresent(s -> {
            options.put("key", s);
        });

        Optional.ofNullable(outputMode).ifPresent(s -> {
            options.put("outputMode", s);
        });

        Optional.ofNullable(ssl).ifPresent(s -> {
            options.put("ssl", s);
        });

        Optional.ofNullable(trigger).ifPresent(s -> {
            options.put("trigger", s);
        });

        Optional.ofNullable(outputDataFormat).ifPresent(s -> {
            options.put("outputDataFormat", s);
        });

        Optional.ofNullable(checkPointLocation).ifPresent(s -> {
            options.put("checkPointLocation", s);
        });

        logger.info(" KafkaOutput options ........ " + options);

        return options;
    }

    public Map getDbmsOutputOptions(String dbUrl, String userName, String password, String schema, String dbTable, String driver) {

        Map options = new HashMap();


        Optional.ofNullable(dbUrl).ifPresent(s -> {
            options.put("dbUrl", s);
        });

        Optional.ofNullable(userName).ifPresent(s -> {
            options.put("userName", s);
        });

        Optional.ofNullable(password).ifPresent(s -> {
            options.put("password", s);
        });

        Optional.ofNullable(schema).ifPresent(s -> {
            options.put("schema", s);
        });

        Optional.ofNullable(dbTable).ifPresent(s -> {
            options.put("dbTable", s);
        });

        Optional.ofNullable(driver).ifPresent(s -> {
            options.put("driver", s);
        });


        logger.info(" DBMSOutput options ........ " + options);

        return options;
    }

    public Map getFileOutputOptions(String format, String fileOutPath, String outputMode, String checkPointLocation) {

        Map options = new HashMap();


        Optional.ofNullable(format).ifPresent(s -> {
            options.put("format", s);
        });

        Optional.ofNullable(fileOutPath).ifPresent(s -> {
            options.put("fileOutPath", s);
        });

        Optional.ofNullable(outputMode).ifPresent(s -> {
            options.put("outputMode", s);
        });

        Optional.ofNullable(checkPointLocation).ifPresent(s -> {
            options.put("checkPointLocation", s);
        });

        logger.info(" Kafka file options ........ " + options);

        return options;
    }

        public DataFrameReader getJdbcReader(Map options) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("DataIngestionApp").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        DataFrameReader dataFrameReader = sparkSession.read();

        options.forEach((k, v) -> logger.info((k + ":" + v)));

        options.forEach((k, v) -> {
            dataFrameReader.option(k.toString(), v.toString());
        });


        return dataFrameReader;
    }

    public DataStreamReader getKafkaReader(Map options) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("DataIngestionApp").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        DataStreamReader dataFrameReader = sparkSession.readStream();

        options.forEach((k, v) -> logger.info((k + ":" + v)));

        options.forEach((k, v) -> {
            dataFrameReader.option(k.toString(), v.toString());
        });


        return dataFrameReader;
    }

    public void getDbmsWriter(Dataset<Row> writer, Map options) throws TimeoutException {

        options.forEach((k, v) -> logger.info((k + ":" + v)));


        property = new Properties();

        property.put("userName",options.get("userName"));
        property.put("password",options.get("password"));
        property.put("driver",options.get("driver"));

        writer.write().jdbc(options.get("dbUrl").toString(),options.get("dbTable").toString(),property);
    }

    public void getKafkaWriter(Dataset<Row> writer, Map options) throws TimeoutException {

        options.forEach((k, v) -> logger.info((k + ":" + v)));

        writer.selectExpr("topic","CAST(key AS STRING)", "CAST(value AS STRING)").writeStream().format("kafka").options(options).option("checkpointLocation", options.get("checkPointLocation").toString()).start();

    }

    public void getKafkaFileWriter(Dataset<Row> writer, Map options) throws TimeoutException, StreamingQueryException {

        options.forEach((k, v) -> logger.info((k + ":" + v)));

        writer.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream().format("csv").option("set","").options(options).option("checkpointLocation", options.get("checkPointLocation").toString()).option("path", options.get("fileOutPath").toString()).start().awaitTermination();




    }

}
