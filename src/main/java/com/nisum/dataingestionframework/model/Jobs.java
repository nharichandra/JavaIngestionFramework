package com.nisum.dataingestionframework.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Jobs implements Serializable {

   @JsonIgnore
   @JsonProperty(value = "mode")
   private String mode = "batch";

   @JsonIgnore
   @JsonProperty(value = "jobs")
   private List<Job> jobs;

   @JsonIgnore
   @JsonProperty(value = "streamParam")
   private StreamParameter streamParam;

}
