package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class Job implements Serializable {

    private String name;
    private String description;

    private List<FileInput> fileInputs;
    private List<FileOutput> fileOutputs;
    private List<DBMSInput> dbmsInputs;
    private List<KafkaInput> kafkaInputs;
    private List<KafkaOutput> kafkaOutputs;
    private List<DBMSOutput> dbmsOutputs;

    private SchemaParameter schemaParams;

}
