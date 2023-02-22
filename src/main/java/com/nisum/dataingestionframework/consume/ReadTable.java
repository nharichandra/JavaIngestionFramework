package com.nisum.dataingestionframework.consume;

import com.nisum.dataingestionframework.dependencies.ColumnTranformation;
import com.nisum.dataingestionframework.dependencies.DateTimeUtilities;
import com.nisum.dataingestionframework.dependencies.SchemaUtilities;
import com.nisum.dataingestionframework.dependencies.ZipUnzipUtility;
import com.nisum.dataingestionframework.model.DBMSInput;
import com.nisum.dataingestionframework.model.DatasetModel;
import lombok.Data;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.util.*;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class ReadTable implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ReadTable.class);

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private SparkSession sparkSession;

    Dataset<Row> dataFrame;
    String dataFormat;
    Set<String> inputDataFrames = new LinkedHashSet<String>();
    SchemaUtilities schemaUtilities = new SchemaUtilities();
    DatasetModel datasetModel = new DatasetModel();
    Map<String, Dataset<Row>> dataSetMap = new HashMap<String, Dataset<Row>>();
    List<DatasetModel> datasetModelList = new ArrayList<DatasetModel>();
    ZipUnzipUtility zipUnzipUtility = new ZipUnzipUtility();
    ColumnTranformation columnTranformation = new ColumnTranformation();
    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();

    public Map<String, Dataset<Row>> tableReader(String dataFrameName, DBMSInput dbmsInput) {

        String url = dbmsInput.getUrl();
        String driver = dbmsInput.getDriver();
        String user = dbmsInput.getUser();
        String password = dbmsInput.getPassword();
        String dbTable = dbmsInput.getDbtable();

        int numPartitions = dbmsInput.getNumPartitions();
        String partitionColumn = dbmsInput.getPartitionColumn();
        String lowerBound = dbmsInput.getLowerBound();
        String upperBound = dbmsInput.getUpperBound();
        int queryTimeout = dbmsInput.getQueryTimeout();
        int fetchsize = dbmsInput.getFetchsize();
        boolean predicatePushdown = dbmsInput.isPushDownPredicate();

        Map options = schemaUtilities.getJdbcOptions(url, user, password, driver, dbTable, numPartitions, partitionColumn, lowerBound, upperBound, queryTimeout, fetchsize, predicatePushdown);

        DataFrameReader reader = schemaUtilities.getJdbcReader(options);

        logger.info("Before Execution of Read DBMSTable " + dateTimeUtilities.getCurrentDateTime());

           dataFrame = reader.format("jdbc").load();

        logger.info("After Execution of Read DBMSTable " + dateTimeUtilities.getCurrentDateTime());

        dataSetMap.put(dataFrameName,dataFrame);

        logger.info(" MapModel Read File  ...." + datasetModel.toString());

        return dataSetMap;
    }

}

