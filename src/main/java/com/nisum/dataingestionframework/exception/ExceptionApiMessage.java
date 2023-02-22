package com.nisum.dataingestionframework.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

public class ExceptionApiMessage implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionApiMessage.class);

   /* private LocalDateTime timestamp;
    private HttpStatus status;*/
    private String message;
   // private List<String> details;


    public String toString() {
        return "ExceptionMessage(message=" + this.message + ")";
    }

    /*public String toString() {
        return "ExceptionMessage(timestamp=" + this.getTimestamp() + ", status=" + this.getStatus() + ", message=" + this.getMessage() + ", details=" + this.getDetails() + ")";
    }*/


}
