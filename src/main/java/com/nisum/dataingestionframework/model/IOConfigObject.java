package com.nisum.dataingestionframework.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class IOConfigObject implements Serializable {
	
	
	private static final long serialVersionUID = 6296141089982542981L;
	private List<InputConfigObject> inputConfigObject;
	private List<OutputConfigObject> outputConfigObject;
}
