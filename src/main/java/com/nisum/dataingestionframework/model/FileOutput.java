package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class FileOutput implements Serializable {

    private String dataFrameName;
    private String format;
    private String fileOutPath;
    private String mode = "Append";
    private boolean header;
    private boolean snakeCaseHeader;
   // private boolean delimiter = false;
   private String delimiter;
    private int numPartitions = 0;
    private List<String> selectCol;
   //private String[] selectCol;
    private List<String> partitionBy;

    private List<ColumnNameTypeLength> addColumns;
    private String checkPointLocation;


}
