package com.nisum.dataingestionframework.model;

import lombok.Data;

@Data
public class DBMSOutput {

    private String dataFrameName;
    private String dbUrl;
    private String userName;
    private String password;
    private String customSchema;
    private String  driver;
    private String dbTable;
}
