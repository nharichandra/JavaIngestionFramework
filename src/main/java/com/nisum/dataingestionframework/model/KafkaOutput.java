package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class KafkaOutput implements Serializable {

    private String dataFrameName;
    private String servers;
    private String key;
    private List<String> topics;
    private String outputMode;
    private String trigger;
    private String outputDataFormat;
    private String checkPointLocation;
    private SslParameter ssl;

}
