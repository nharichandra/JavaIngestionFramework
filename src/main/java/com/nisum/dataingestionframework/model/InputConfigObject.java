package com.nisum.dataingestionframework.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class InputConfigObject implements Serializable {
 
	private static final long serialVersionUID = 1992695228755314017L;
private String dataFrameName;
 private String fileInPath;
 private String archivalPath;
 private DataFormat dataFormat; 
 
}
