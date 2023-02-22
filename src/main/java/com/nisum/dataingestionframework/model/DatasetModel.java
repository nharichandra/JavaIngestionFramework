package com.nisum.dataingestionframework.model;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class DatasetModel implements Serializable {

   // private Map<String, Dataset<Row>> dataFrameDataset = null;

    private String dataFrameName;
    private Dataset<Row> dataset;


}
