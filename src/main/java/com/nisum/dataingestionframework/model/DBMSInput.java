package com.nisum.dataingestionframework.model;


import lombok.Data;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class DBMSInput implements Serializable {

    private String dataFrameName;
    private String url;
    private String user;
    private String password;
    private String customSchema;
    private String  driver;
    private String dbtable;
    //private DBMSIncremental  dbIncremental = "None";
    private DBMSIncremental  dbIncremental;
    private int numPartitions;
    private String partitionColumn;
    private String lowerBound;
    private String upperBound;
    private int queryTimeout;
    private int fetchsize;
    /*batchsize: int = 1000  -- for insert
    truncate: boo; = False -- for truncate*/
    private boolean pushDownPredicate;

}
