package com.nisum.dataingestionframework.config;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Configuration
@PropertySource("classpath:application.properties")
public class EnableSparkConfiguration implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(EnableSparkConfiguration.class);

    @Autowired
    private Environment env;

    @Value("${app.name:JavaIngestionFramework}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local");
        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext(){

        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession(){
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName("SpringBoot Spark Ingestion Framework")
                .getOrCreate();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer(){
        return new PropertySourcesPlaceholderConfigurer();
    }
}
