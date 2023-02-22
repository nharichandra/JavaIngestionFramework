package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class SchemaParameter implements Serializable {

   private String schemaFilePath;
    private List<String> schemaFiles;
    private String schemaColumnName = "None";

}
