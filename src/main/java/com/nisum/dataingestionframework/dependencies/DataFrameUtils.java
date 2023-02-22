package com.nisum.dataingestionframework.dependencies;

import com.nisum.dataingestionframework.model.ColumnNameTypeLength;
import com.nisum.dataingestionframework.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class DataFrameUtils implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DataFrameUtils.class);

    String columnValue;
    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();

    public Dataset<Row> dataframeAddColumns(Dataset<Row> dataFrame, List<ColumnNameTypeLength> addColumns) {

        Iterator iterator = addColumns.iterator();

        while(iterator.hasNext()) {

            ColumnNameTypeLength columnNameTypeLength =  (ColumnNameTypeLength)iterator.next();


            if (columnNameTypeLength.getColumnValue().length() > 0) {
                logger.info("getting column value from json request");
                columnValue = columnNameTypeLength.getColumnValue();
            }

            if (columnNameTypeLength.getColumnValue().equalsIgnoreCase(Constants.CURRENTDATE)) {
                logger.info("getting current date from json request");

                columnValue = dateTimeUtilities.getCurrentDate().toString();
            }
            if (columnNameTypeLength.getColumnValue().equalsIgnoreCase(Constants.EPOCH)) {
                logger.info("getting epoch of current time from json request");

                columnValue = dateTimeUtilities.getEpoch().toString();
            }

            dataFrame = dataFrame.withColumn(
                    columnNameTypeLength.getColumnName(),
                    functions.lit(columnValue)
            );


        }
        return dataFrame;
    }

}
