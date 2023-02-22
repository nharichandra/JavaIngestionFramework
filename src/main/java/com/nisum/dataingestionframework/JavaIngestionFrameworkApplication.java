package com.nisum.dataingestionframework;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.concurrent.TimeoutException;

/* @author Hari Chandra Prasad Nimmagadda */

@SpringBootApplication
@EnableSwagger2
@OpenAPIDefinition(info =
	@Info(title = "Jobs API", version = "1.0", description = "Documentation Jobs API v1.0")
)
public class JavaIngestionFrameworkApplication {

	private static final Logger logger = LoggerFactory.getLogger(JavaIngestionFrameworkApplication.class);

	public static void main(String[] args) throws TimeoutException {

	//	System.setProperty("hadoop.home.dir", "D:/Configuration/hadoop-2.7.3");
		SpringApplication.run(JavaIngestionFrameworkApplication.class, args);


	}

}
