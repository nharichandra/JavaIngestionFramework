package com.nisum.dataingestionframework.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;

/* @author Hari Chandra Prasad Nimmagadda */

public class ErrorDetails implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ErrorDetails.class);

    private Date timestamp;
    private String message;
    private String details;

    public ErrorDetails(Date timestamp, String message, String details) {
        super();
        this.timestamp = timestamp;
        this.message = message;
        this.details = details;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

    public String getDetails() {
        return details;
    }
}
