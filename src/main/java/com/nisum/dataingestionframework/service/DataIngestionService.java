package com.nisum.dataingestionframework.service;

import com.nisum.dataingestionframework.config.ReadFile;
import com.nisum.dataingestionframework.config.WriteFile;
import com.nisum.dataingestionframework.consume.ReadTable;
import com.nisum.dataingestionframework.consume.StreamReader;
import com.nisum.dataingestionframework.model.*;
import com.nisum.dataingestionframework.publish.StreamWriter;
import com.nisum.dataingestionframework.publish.TableWriter;
import com.nisum.dataingestionframework.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/* @author Hari Chandra Prasad Nimmagadda */

@Service
public class DataIngestionService implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);

	ReadFile readFile;
	ReadTable readTable;
	WriteFile writeFile;
	StreamReader streamReader;
	StreamWriter streamWriter;
	TableWriter tableWriter;
    List<Job> jobList = null;

    public List<Job> getDataIngestionJobs(Jobs jobs) {

        logger.info("Enter into the DataIngestionService getDataIngestionJobs Method ....... " + jobs.toString());

        if(jobs.getMode().equalsIgnoreCase(Constants.batchJobMode)) {

            logger.debug("Enter into the Batch Job Mode .................");

            jobList =   jobs.getJobs().stream()
                    .map(job ->  typeOfInput(job))
                    .collect(Collectors.toList());

        } else if(jobs.getMode().equalsIgnoreCase(Constants.streamJobMode)) {

            logger.debug("Enter into the Stream Job Mode .................");

        }

        logger.debug("End of the DataIngestionService getDataIngestionJobs Method ....... " + jobList);

        return jobList;
    }

    public Job typeOfInput(Job job) {

        readFile = new ReadFile();
        writeFile = new WriteFile();
        readTable = new ReadTable();
        streamReader = new StreamReader();
        streamWriter = new StreamWriter();
        tableWriter = new TableWriter();


        System.out.println(job);

        if( job.getFileInputs()!=null  && !job.getFileInputs().isEmpty() ) {
            Optional.of(job).filter(s -> {

            Optional.ofNullable(s.getFileInputs()).ifPresent(fileInputs ->  {

                Map<String, FileInput> fileInputMap = new HashMap<>();
                job.getFileInputs().stream().forEach(fileInput -> {
                    fileInputMap.put(fileInput.getDataFrameName(), fileInput);
                });

                logger.info("FileInputMap ........ " + fileInputMap);

                Map<String, Dataset<Row>>  datasetMap = new HashMap<>();
                fileInputMap.entrySet().stream().forEach(stringFileInputEntry -> {
                    Map<String, Dataset<Row>> persistedDataSetMap =  readFile.fileReader(stringFileInputEntry.getKey(), stringFileInputEntry.getValue() ) ;
                    datasetMap.putAll(persistedDataSetMap);
                });

                logger.info(" File Input Data mapModelData  ....... " + datasetMap.toString());
            if(job.getFileOutputs()!=null) {
                Map<String, FileOutput> fileOutputMap = new HashMap<>();

                job.getFileOutputs().stream().forEach(fileOutput -> {
                    fileOutputMap.put(fileOutput.getDataFrameName(), fileOutput);
                });
                logger.info("fileOutputMap ..... " + fileOutputMap);

                fileOutputMap.forEach((k, v) -> writeFile.fileWriter(k, v, datasetMap));
            }
             if(job.getDbmsOutputs()!=null) {
                 Map<String, DBMSOutput> dbmsOutputMap = new HashMap<>();

                 job.getDbmsOutputs().stream().forEach(dbmsOutput -> {
                     dbmsOutputMap.put(dbmsOutput.getDataFrameName(), dbmsOutput);
                 });
                 logger.info("dbmsOutputMap ..... " + dbmsOutputMap);

                 dbmsOutputMap.forEach((k, v) -> tableWriter.jdbcWriter(k, v, datasetMap));
             }
            });

            return true;

        });
		
		} else if(job.getDbmsInputs() != null && !job.getDbmsInputs().isEmpty()){

            Optional.of(job).filter(s ->  {
			
			 if(s.getDbmsInputs().size() != 0 || job.getFileOutputs().size() != 0) {

                 Map<String, DBMSInput> dbmsInputMap = new HashMap<>();
                 job.getDbmsInputs().stream().forEach(dbmsInput -> {
                     dbmsInputMap.put(dbmsInput.getDataFrameName(), dbmsInput);
                 });

                 logger.info(" DBMSInput ........ " + dbmsInputMap);

                 Map<String, Dataset<Row>> datasetMap = new HashMap<>();
                 dbmsInputMap.entrySet().stream().forEach(stringFileInputEntry -> {
                     Map<String, Dataset<Row>> persistedDataSetMap = readTable.tableReader(stringFileInputEntry.getKey(), stringFileInputEntry.getValue());
                     datasetMap.putAll(persistedDataSetMap);
                 });


                 logger.info(" DBMS Input Data mapModelData  ....... " + datasetMap.toString());

                 Map<String, FileOutput> fileOutputMap = new HashMap<>();

                 job.getFileOutputs().stream().forEach(fileOutput -> {
                     fileOutputMap.put(fileOutput.getDataFrameName(), fileOutput);
                 });
                 logger.info("fileOutputMap ..... " + fileOutputMap);

                 fileOutputMap.forEach((k, v) -> writeFile.fileWriter(k, v, datasetMap));

             }
                return true;
            });

		}
		
		else if(job.getKafkaInputs() != null && !job.getKafkaInputs().isEmpty()){
            Optional.of(job).filter(s -> {

                if (s.getKafkaInputs().size() != 0 || job.getFileOutputs().size() != 0) {

                    Map<String, KafkaInput> kafkaInputMap = new HashMap<>();
                    job.getKafkaInputs().stream().forEach(kafkaInput -> {
                        kafkaInputMap.put(kafkaInput.getDataFrameName(), kafkaInput);
                    });

                    logger.info(" KafkaInput ........ " + kafkaInputMap);

                    Map<String, Dataset<Row>> datasetMap = new HashMap<>();
                    kafkaInputMap.entrySet().stream().forEach(stringFileInputEntry -> {
                        Map<String, Dataset<Row>> persistedDataSetMap = null;
                        try {
                            persistedDataSetMap = streamReader.tableReader(stringFileInputEntry.getKey(), stringFileInputEntry.getValue());
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                        }
                        logger.info(" persistedDataSetMap  ........ " + persistedDataSetMap.get(stringFileInputEntry.getKey()));

                        datasetMap.putAll(persistedDataSetMap);
                    });


                    logger.info(" Kafka Input Data mapModelData  ....... " + datasetMap.toString());
                    if (job.getKafkaOutputs() != null) {

                        Map<String, KafkaOutput> kafkaOutputMap = new HashMap<>();

                        job.getKafkaOutputs().stream().forEach(kafkaOutput -> {
                            logger.info(" kafkaOutput  ....... " + kafkaOutput);

                            kafkaOutputMap.put(kafkaOutput.getDataFrameName(), kafkaOutput);
                        });
                        logger.info("kafkaOutputMap ..... " + kafkaOutputMap);

                        kafkaOutputMap.forEach((k, v) -> streamWriter.streamWrite(k, v, datasetMap));

                    }

                    if (job.getFileOutputs() != null) {
                        Map<String, FileOutput> fileOutputMap = new HashMap<>();

                        job.getFileOutputs().stream().forEach(fileOutput -> {
                            logger.info(" kafkafileOutput  ....... " + fileOutput);

                            fileOutputMap.put(fileOutput.getDataFrameName(), fileOutput);
                        });
                        logger.info("kafkafileOutputMap ..... " + fileOutputMap);

                        fileOutputMap.forEach((k, v) -> streamWriter.fileWrite(k, v, datasetMap));

                    }
                }
                return true;
            });

        }


        return job;

    }


    /*public Job typeOfInput(Job job) {

        readFile = new ReadFile();
        writeFile = new WriteFile();

        if(job.getFileInputs().size() != 0 || job.getFileOutputs().size() != 0) {

            Map<String, FileInput> fileInputMap = new HashMap<>();
            job.getFileInputs().stream().forEach(fileInput -> {
                fileInputMap.put(fileInput.getDataFrameName(), fileInput);
            });

            logger.info("FileInputMap ........ " + fileInputMap);

            Map<String, Dataset<Row>>  datasetMap = new HashMap<>();
            fileInputMap.entrySet().stream().forEach(stringFileInputEntry -> {
                Map<String, Dataset<Row>> persistedDataSetMap =  readFile.fileReader(stringFileInputEntry.getKey(), stringFileInputEntry.getValue() ) ;
                datasetMap.putAll(persistedDataSetMap);
            });

            logger.info(" File Input Data mapModelData  ....... " + datasetMap.toString());

            Map<String, FileOutput> fileOutputMap = new HashMap<>();

            job.getFileOutputs().stream().forEach(fileOutput ->  {
                fileOutputMap.put(fileOutput.getDataFrameName(), fileOutput);
            });
            logger.info("fileOutputMap ..... " + fileOutputMap);

            fileOutputMap.forEach((k,v) -> writeFile.fileWriter(k, v, datasetMap));

            }

        return job;

    }
*/

    }
