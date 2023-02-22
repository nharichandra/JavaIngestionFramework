package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class KafkaInput implements Serializable {
   private String dataFrameName;
    private String servers;
   private List<String> topics;
   private String consumerGroup;
   private String subscribe;
   private String startingOffsets = "latest";
  // private String endingOffsets = "latest";
   private boolean autoCommit= false;
   private int autoCommitInterval = 2000;
   private boolean failOnDataLoss = false;
   private SslParameter ssl;

}


