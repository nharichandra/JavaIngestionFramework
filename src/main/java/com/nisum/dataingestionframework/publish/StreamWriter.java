package com.nisum.dataingestionframework.publish;

import com.nisum.dataingestionframework.dependencies.DateTimeUtilities;
import com.nisum.dataingestionframework.dependencies.SchemaUtilities;
import com.nisum.dataingestionframework.model.FileOutput;
import com.nisum.dataingestionframework.model.KafkaOutput;
import com.nisum.dataingestionframework.model.SslParameter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class StreamWriter implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(StreamWriter.class);

    SchemaUtilities schemaUtilities = new SchemaUtilities();

    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();

    public void streamWrite(String dataFrameName, KafkaOutput kafkaOutput,Map<String, Dataset<Row>>  datasetMap) {
        datasetMap.entrySet().stream().forEach(datasetModel -> {

            if (datasetModel.getKey().equalsIgnoreCase(dataFrameName)) {

                Dataset<Row> dataFrame = datasetModel.getValue();

                String servers = kafkaOutput.getServers();
                List<String> topics = kafkaOutput.getTopics();
                SslParameter ssl = kafkaOutput.getSsl();
                String key = kafkaOutput.getKey();
                String outputMode = kafkaOutput.getOutputMode();
                String trigger = kafkaOutput.getTrigger();
                String outputDataFormat = kafkaOutput.getOutputDataFormat();
                String checkPointLocation = kafkaOutput.getCheckPointLocation();
                logger.info("Before Execution of kafka stream data " + dateTimeUtilities.getCurrentDateTime());

                Map options = schemaUtilities.getKafkaOutputOptions(servers, topics, key, outputMode, ssl, trigger, outputDataFormat, checkPointLocation);


                try {
                    schemaUtilities.getKafkaWriter(dataFrame, options);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }


                logger.info("After Execution of kafka stream data " + dateTimeUtilities.getCurrentDateTime());


            }
            });

    }

    public void fileWrite(String dataFrameName, FileOutput fileOutput, Map<String, Dataset<Row>>  datasetMap) {
        datasetMap.entrySet().stream().forEach(datasetModel -> {

            if (datasetModel.getKey().equalsIgnoreCase(dataFrameName)) {

                Dataset<Row> dataFrame = datasetModel.getValue();


                String fileFormat = fileOutput.getFormat();
                String fileOutPath =  fileOutput.getFileOutPath();
                String outputMode = fileOutput.getMode();
                String checkPointLocation = fileOutput.getCheckPointLocation();



                Map options = schemaUtilities.getFileOutputOptions(fileFormat, fileOutPath, outputMode, checkPointLocation);


                try {
                    schemaUtilities.getKafkaFileWriter(dataFrame, options);
                } catch (TimeoutException | StreamingQueryException e) {
                    e.printStackTrace();
                }



                logger.info("After Execution of kafka stream data " + dateTimeUtilities.getCurrentDateTime());


            }
        });

    }

}
