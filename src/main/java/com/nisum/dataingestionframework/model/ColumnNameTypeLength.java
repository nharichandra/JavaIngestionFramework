package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class ColumnNameTypeLength implements Serializable {

    private String columnName;
    private String columnType;
    private String columnValue;
    private int columnLength = 1;


}
