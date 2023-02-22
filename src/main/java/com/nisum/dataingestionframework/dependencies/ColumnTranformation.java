package com.nisum.dataingestionframework.dependencies;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

/* @author Hari Chandra Prasad Nimmagadda */

public class ColumnTranformation implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ColumnTranformation.class);

    public Dataset<Row> snakeCaseHeaderFun(Dataset<Row> dataframe) {

        return dataframe.select(Arrays.asList(dataframe.columns()).stream()
                .map(x -> col(x).as(x.toLowerCase().replace(" ", "_")))
                .toArray(size -> new Column[size]));

    }



}
