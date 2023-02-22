package com.nisum.dataingestionframework.publish;

import com.nisum.dataingestionframework.dependencies.DateTimeUtilities;
import com.nisum.dataingestionframework.dependencies.SchemaUtilities;
import com.nisum.dataingestionframework.model.DBMSOutput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class TableWriter {
    private static final Logger logger = LoggerFactory.getLogger(TableWriter.class);

    SchemaUtilities schemaUtilities = new SchemaUtilities();

    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();

    public void jdbcWriter(String dataFrameName, DBMSOutput dbmsOutput, Map<String, Dataset<Row>> datasetMap) {
        datasetMap.entrySet().stream().forEach(datasetModel -> {

            if (datasetModel.getKey().equalsIgnoreCase(dataFrameName)) {

                Dataset<Row> dataFrame = datasetModel.getValue();

                String dbUrl = dbmsOutput.getDbUrl();
                String userName = dbmsOutput.getUserName();
                String password = dbmsOutput.getPassword();
                String schema = dbmsOutput.getCustomSchema();
                String dbTable = dbmsOutput.getDbTable();
                String driver = dbmsOutput.getDriver();
                logger.info("Before Execution of dbms data " + dateTimeUtilities.getCurrentDateTime());

                Map options = schemaUtilities.getDbmsOutputOptions(dbUrl, userName, password, schema, dbTable, driver);


                try {
                    schemaUtilities.getDbmsWriter(dataFrame, options);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }


                logger.info("After Execution of dbms data " + dateTimeUtilities.getCurrentDateTime());


            }
        });

    }

}
