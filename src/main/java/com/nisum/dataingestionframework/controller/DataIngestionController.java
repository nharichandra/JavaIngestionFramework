package com.nisum.dataingestionframework.controller;

import com.nisum.dataingestionframework.exception.ExceptionApiMessage;
import com.nisum.dataingestionframework.exception.ResourceNotFoundException;
import com.nisum.dataingestionframework.service.DataIngestionService;
import com.nisum.dataingestionframework.model.Jobs;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* @author Hari Chandra Prasad Nimmagadda */

@RestController
@RequestMapping("/api/v1/dif/jobs")
@Api(value = "Data Ingestion Framework", tags = "Operations pertaining to Jobs in Data Ingestion Framework")
public class DataIngestionController {

	private static final Logger logger = LoggerFactory.getLogger(DataIngestionController.class);

	@Autowired
    DataIngestionService dataIngestionService;

	@ApiOperation(value = "Add Jobs")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Successfully retreived list", response = ExceptionApiMessage.class),
			@ApiResponse(code = 401, message = "You are not Authorized to view the resource", response = ExceptionApiMessage.class),
			@ApiResponse(code = 403, message = "Accessing the resource you wer trying to reach is forbidden", response = ExceptionApiMessage.class),
			@ApiResponse(code = 404, message = "The Resource you are trying to reach is not Found", response = ExceptionApiMessage.class)
	})
	@PostMapping
	private ResponseEntity<Jobs> createJob(
			@ApiParam(value = "Order object store in DataBase", required = true)
			@RequestBody Jobs jobs) throws ResourceNotFoundException {

		logger.debug("Enter into the DataIngestionController Create Jobs Method ....... ");

		if (jobs == null) {
			logger.debug("Jobs Request does not exists");

			return new ResponseEntity<Jobs>(HttpStatus.NOT_FOUND);
		}

		return	Optional.ofNullable(dataIngestionService.getDataIngestionJobs(jobs))
					.map(e -> new ResponseEntity(e, HttpStatus.CREATED))
					.orElseThrow(() -> new ResourceNotFoundException("Jobs Creation Failed"));
		}



	}
