package com.nisum.dataingestionframework.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OutputConfigObject implements Serializable{

	private static final long serialVersionUID = 6636349488865384718L;
	
	private String dataFrameName;
	private String format;
	private String fileOutPath;
	private String mode;
	private String header;
	private String delimiter;
}
