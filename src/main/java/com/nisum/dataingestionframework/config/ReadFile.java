package com.nisum.dataingestionframework.config;

import com.nisum.dataingestionframework.dependencies.DateTimeUtilities;
import com.nisum.dataingestionframework.dependencies.SchemaUtilities;
import com.nisum.dataingestionframework.dependencies.ZipUnzipUtility;
import com.nisum.dataingestionframework.exception.ResourceNotFoundException;
import com.nisum.dataingestionframework.model.ColumnNameTypeLength;
import com.nisum.dataingestionframework.model.DatasetModel;
import com.nisum.dataingestionframework.model.FileInput;
import com.nisum.dataingestionframework.util.Constants;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/* @author Hari Chandra Prasad Nimmagadda */

public class ReadFile implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ReadFile.class);

    Dataset<Row> dataFrame;
    Dataset<Row> newdf;
    SchemaUtilities schemaUtilities = new SchemaUtilities();
    DatasetModel datasetModel = new DatasetModel();
    Map<String, Dataset<Row>> dataSetMap = new HashMap<String, Dataset<Row>>();
    //StructType schema = new StructType();
    SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("DataIngestionApp2").getOrCreate();


    ZipUnzipUtility zipUnzipUtility = new ZipUnzipUtility();
    DateTimeUtilities dateTimeUtilities = new DateTimeUtilities();

   public Map<String, Dataset<Row>> fileReader(String dataFrameName, FileInput fileInput) {

        boolean header = fileInput.getDataFormat().isHeader();
        String delimiter = fileInput.getDataFormat().getDelimiter();
        boolean inferschema = fileInput.getDataFormat().isInferSchema();
        boolean multiline = fileInput.getDataFormat().isMultiLine();
        String roottag = fileInput.getDataFormat().getRoottag();
        String rowtag = fileInput.getDataFormat().getRowtag();
        Optional<String> archivalPath = Optional.ofNullable(fileInput.getArchivalPath());

       String fileFormat = fileInput.getDataFormat().getDataFormat().toLowerCase();
       String archivePath = fileInput.getArchivalPath();

       Map options =  schemaUtilities.getOptions(header, delimiter, inferschema, multiline, roottag, rowtag);

       DataFrameReader reader = schemaUtilities.getFileReader(fileInput.getEnforceSchemaFile(), options);

        String path = fileInput.getFileInPath();

       if (fileInput.getNumPartitions() > 0)
           dataFrame = dataFrame.repartition(fileInput.getNumPartitions());

       fileInput.getSelectCol().stream()
                        .map(s -> dataFrame = dataFrame.select(s)
                            );

      archivalPath.ifPresent(s ->
              {
                  try {
                      zipUnzipUtility.unzip(s, archivePath);
                  } catch (Exception e) {
                      e.printStackTrace();
                  }
              }
              );

        logger.info("Before Execution of Read File " + dateTimeUtilities.getCurrentDateTime());

       if (fileFormat.equalsIgnoreCase(Constants.CSV))
         // dataFrame = reader.schema(schemaUtilities.getCsvSchema()).csv(path);
           dataFrame = reader.csv(path);
        else if(fileFormat.equalsIgnoreCase(Constants.JSON))
            dataFrame = reader.json(path);
        else if(fileFormat.equalsIgnoreCase(Constants.PARQUET))
            dataFrame = reader.parquet(path);
        else if(fileFormat.equalsIgnoreCase(Constants.DELTA))
            dataFrame = reader.format(Constants.DELTA).load(path);
        else if (fileFormat.equalsIgnoreCase(Constants.XML))
           dataFrame = reader.format("com.databricks.spark.xml").load(path);
        else if (fileFormat.equalsIgnoreCase(Constants.FIXED)){
          logger.info("Fixed");
             dataFrame = reader.text(path);
            //logger.info("Fixed");
            dataFrame = read_fixed_file(dataFrame,fileInput);}
        else if(fileFormat.equalsIgnoreCase(Constants.AVRO))
            dataFrame = reader.format(Constants.AVRO).load(path);
         else
           try {
               throw new ResourceNotFoundException(" Unknown data format! ");
           } catch (ResourceNotFoundException e) {
               e.printStackTrace();
           }

       logger.info("After Execution of Read File " + dateTimeUtilities.getCurrentDateTime());
       dataSetMap.put(dataFrameName,dataFrame);

       logger.info(" MapModel Read File  ...." + datasetModel.toString());

       return dataSetMap;
    }

    public Dataset<Row> read_fixed_file(Dataset<Row> dataFrame,FileInput fileInput){
        List<ColumnNameTypeLength> columnNameTypeLengthList = fileInput.getDataFormat().getColNameTypeLength();

        //StructField[] myArr = new StructField[];
        if(columnNameTypeLengthList != null && !columnNameTypeLengthList.isEmpty()){

             StructType schema = new StructType(columnNameTypeLengthList.stream().map(column -> {
                 return new StructField(column.getColumnName(), inferType(column.getColumnType()), true, Metadata.empty());
            }).toArray(StructField[]::new));
            System.out.println("before dataframe.show()");

            System.out.println("Schema for final df : " + schema.length());
            System.out.println("Schema for final df : " + schema.fields());

            JavaRDD parsedRdd = dataFrame.toJavaRDD().map(x -> {
                String str = x.getString(0);
                Integer startIndex = 0;
                List<Object> columnsDataList = new ArrayList<>();
                for (ColumnNameTypeLength column : columnNameTypeLengthList) {
                    Integer currPos = column.getColumnLength();
                    String columnData = "";
                    columnData = str.substring(startIndex, startIndex + currPos);
                    startIndex += currPos;
                    columnsDataList.add(getTypedData(columnData.trim(), column.getColumnType()));
                }
                Row row = RowFactory.create(columnsDataList.toArray());
                return row;
            });
            Dataset<Row> newdf = sparkSession.createDataFrame(parsedRdd,schema);



            newdf.printSchema();

            newdf.show();
            return newdf;

        };

        return newdf;
    }

    public Object getTypedData(String columnData, String type) {
        switch (type) {

            case "int":
                return Integer.parseInt(columnData);
            case "long":
                return Long.parseLong(columnData);
            case "str":
                return columnData;
            case "float":
                return Float.parseFloat(columnData);
            case "double":
                return Double.parseDouble(columnData);
            case "boolean":
                return Boolean.parseBoolean(columnData);
            case "date":
                return java.sql.Date.valueOf(columnData);
            default:
                return columnData;

        }


    }


    public DataType inferType(String columnType) {
        switch (columnType) {

            case "int":
                return DataTypes.IntegerType;
            case "long":
                return DataTypes.LongType;
            case "str":
                return DataTypes.StringType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "boolean":
                return DataTypes.BooleanType;
            case "date":
                return DataTypes.DateType;
            default:
                return DataTypes.StringType;
        }
    }
}
