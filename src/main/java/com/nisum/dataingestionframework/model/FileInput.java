package com.nisum.dataingestionframework.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class FileInput implements Serializable {

    private String dataFrameName;
    private String fileInPath;
    private String archivalPath;
    private DataFormat dataFormat;
    private boolean snakeCaseHeader;
    private List<String> selectCol;
    private int numPartitions = 0;

    @JsonIgnore
    @JsonProperty(value = "enforceSchemaFile")
    private String enforceSchemaFile;

}
