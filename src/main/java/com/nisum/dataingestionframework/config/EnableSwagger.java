package com.nisum.dataingestionframework.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/* @author Hari Chandra Prasad Nimmagadda */

@Configuration
@EnableSwagger2
public class EnableSwagger {

    private static final Logger logger = LoggerFactory.getLogger(EnableSwagger.class);

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2).select()
                .apis(RequestHandlerSelectors
                        .basePackage("com.nisum.dataingestionframework.controller"))
                .paths(PathSelectors.regex("/.*"))
                .build().apiInfo(apiEndPointsInfo());
    }
    private ApiInfo apiEndPointsInfo() {
        return new ApiInfoBuilder().title("Data Ingestion Framework REST API")
                .description("Data Ingestion Framework REST API")
                .contact(new Contact("Nisum Solutions", "www.nisum.com", "cnimmagadda@nisum.com"))
                .license("Nisum 1.0")
                .licenseUrl("http://www.nisum.com/licenses/LICENSE-1.0.html")
                .version("1.0.0")
                .build();
    }
}
