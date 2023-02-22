package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class DataFormat implements Serializable {

	private String dataFormat;
	private boolean header;
	private String delimiter;
	private boolean inferSchema;
	private boolean multiLine;
	private String roottag = "None";
	private String rowtag = "None";

	private List<ColumnNameTypeLength> colNameTypeLength;

}