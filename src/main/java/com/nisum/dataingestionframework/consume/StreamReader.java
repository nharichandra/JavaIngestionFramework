package com.nisum.dataingestionframework.consume;

import com.nisum.dataingestionframework.dependencies.DateTimeUtilities;
import com.nisum.dataingestionframework.dependencies.SchemaUtilities;
import com.nisum.dataingestionframework.model.DatasetModel;
import com.nisum.dataingestionframework.model.KafkaInput;
import com.nisum.dataingestionframework.model.SslParameter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class StreamReader implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);

    Dataset<Row> dataFrame;
    SchemaUtilities schemaUtilities = new SchemaUtilities();
    DatasetModel datasetModel = new DatasetModel();
    Map<String, Dataset<Row>> dataSetMap = new HashMap<String, Dataset<Row>>();
    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();

    public Map<String, Dataset<Row>> tableReader(String dataFrameName, KafkaInput kafkaInput) throws TimeoutException {

        String servers = kafkaInput.getServers();
        List<String> topics = kafkaInput.getTopics();

        String consumerGroup = kafkaInput.getConsumerGroup();
        SslParameter ssl = kafkaInput.getSsl();
        String startingOffsets = kafkaInput.getStartingOffsets();
        String subscribe = kafkaInput.getSubscribe();
        boolean autoCommit= false;
        int autoCommitInterval = kafkaInput.getAutoCommitInterval();
        boolean failOnDataLoss = false;

        Map options = schemaUtilities.getKafkaOptions(servers, topics,subscribe, consumerGroup, ssl, startingOffsets, autoCommit, autoCommitInterval, failOnDataLoss);

        DataStreamReader reader = schemaUtilities.getKafkaReader(options);

        logger.info("Before Execution of kafka stream data " + dateTimeUtilities.getCurrentDateTime());

        dataFrame = reader.format("kafka").load();

        dataFrame.writeStream().foreach(new ForeachWriter<Row>() {
            @Override
            public boolean open(long partitionId, long epochId) {
                return true;
            }

            @Override
            public void process(Row value) {
                value.get(1);
            }

            @Override
            public void close(Throwable errorOrNull) {

            }

        }).start();

        logger.info("After Execution of kafka stream data " + dateTimeUtilities.getCurrentDateTime());

        dataSetMap.put(dataFrameName,dataFrame);

        logger.info(" MapModel Read File  ...." + datasetModel.toString());

        return dataSetMap;

    }
    }
