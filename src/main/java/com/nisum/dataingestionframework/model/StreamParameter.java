package com.nisum.dataingestionframework.model;

import lombok.Data;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class StreamParameter implements Serializable {

    private int batchDuration = 2000;
    private boolean stopStreamGracefully = false;
    private int timeOut;

}
