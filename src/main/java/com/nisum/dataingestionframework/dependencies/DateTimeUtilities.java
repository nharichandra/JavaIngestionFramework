package com.nisum.dataingestionframework.dependencies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/* @author Hari Chandra Prasad Nimmagadda */

public class DateTimeUtilities implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DateTimeUtilities.class);

    LocalDate localDate;
    LocalDateTime localDateTime;

    public LocalDate getCurrentDate() {

        return LocalDate.now();
    }

    public LocalTime getEpoch() {

       return LocalTime.now();
    }

    public LocalDateTime getCurrentDateTime() {

        return LocalDateTime.now();

    }


}
