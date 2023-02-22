package com.nisum.dataingestionframework.config;

import com.nisum.dataingestionframework.dependencies.ColumnTranformation;
import com.nisum.dataingestionframework.dependencies.DataFrameUtils;
import com.nisum.dataingestionframework.dependencies.DateTimeUtilities;
import com.nisum.dataingestionframework.dependencies.SchemaUtilities;
import com.nisum.dataingestionframework.exception.ResourceNotFoundException;
import com.nisum.dataingestionframework.model.FileOutput;
import com.nisum.dataingestionframework.util.Constants;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/* @author Hari Chandra Prasad Nimmagadda */

public class WriteFile implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(WriteFile.class);

    DataFrameUtils dataFrameUtils = new DataFrameUtils();
    SchemaUtilities schemaUtilities = new SchemaUtilities();
    ColumnTranformation columnTranformation = new ColumnTranformation();
    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();
    String res = new String();

    public String fileWriter(String dataFrameName, FileOutput fileOutput, Map<String, Dataset<Row>> datasetModelMap) {

        logger.info(" mapModel file writer  .... " + datasetModelMap.toString());

        datasetModelMap.entrySet().stream().forEach(datasetModel -> {

            if (datasetModel.getKey().equalsIgnoreCase(dataFrameName)) {

                Dataset<Row> dataFrame = datasetModel.getValue();

                if (fileOutput.getSelectCol().size() > 0) {

                    Column[] selectedColumns = new Column[fileOutput.getSelectCol().size()];
                    for (int i = 0; i < selectedColumns.length; i++) {
                        selectedColumns[i] = new Column(fileOutput.getSelectCol().get(i));
                    }

                    dataFrame = dataFrame.select(selectedColumns);

                }

                //  Add Columns
                if (fileOutput.getAddColumns().size() > 0) {
                    dataFrame = dataFrameUtils.dataframeAddColumns(dataFrame, fileOutput.getAddColumns());
                }

                //  SnakeCaseHeader
                if (fileOutput.isSnakeCaseHeader())
                    dataFrame = columnTranformation.snakeCaseHeaderFun(dataFrame);

                // Repartition dataframe
                if (fileOutput.getNumPartitions() > 0)
                    dataFrame = dataFrame.repartition(fileOutput.getNumPartitions());

                //  Partition By column list

             if (fileOutput.getPartitionBy()!=null) {
                 List<String> list = fileOutput.getPartitionBy();
                 String delim = ",";

                  res = String.join(delim, list);
                }

                boolean header = fileOutput.isHeader();
                String delimiter = fileOutput.getDelimiter();

                Map optionsMap = schemaUtilities.getOptions(header, delimiter);

                String fileFormat = fileOutput.getFormat().toLowerCase();
                String path = fileOutput.getFileOutPath();

                logger.info("Before Execution of Write File " + dateTimeUtilities.getCurrentDateTime());

                dataFrame.show();
                if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.CSV)) {
                    dataFrame.write().options(optionsMap).partitionBy(res).csv(path);
                } else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.JSON))
                    dataFrame.write().json(path);
                else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.PARQUET))
                    dataFrame.write().parquet(path);
                else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.AVRO))
                    dataFrame.write().format(Constants.AVRO).save(path);
                else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.DELTA))
                    dataFrame.write().format(Constants.DELTA).save(path);
                else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.XML))
                    dataFrame.write().format("com.databricks.spark.xml").save(path);
                else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.TEXT))
                    dataFrame.write().text(path);
                else if (fileFormat.toLowerCase().equalsIgnoreCase(Constants.FIXED))
                    dataFrame.write().options(optionsMap).csv(path);
                else
                    try {
                        throw new ResourceNotFoundException(" Unknown data write format! ");
                    } catch (ResourceNotFoundException e) {
                        e.printStackTrace();
                    }
            }



        });

            logger.info("After Execution of Write File " + dateTimeUtilities.getCurrentDateTime());


        return "Failed to write data";
    }

}
