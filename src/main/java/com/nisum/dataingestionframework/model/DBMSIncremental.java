package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class DBMSIncremental implements Serializable {

    private String maxQuery;
    private String auditTablePath;
    private String ingestionID;

}
