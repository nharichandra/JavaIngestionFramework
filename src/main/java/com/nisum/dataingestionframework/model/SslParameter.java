package com.nisum.dataingestionframework.model;


import lombok.Data;

import java.io.Serializable;

/* @author Hari Chandra Prasad Nimmagadda */

@Data
public class SslParameter implements Serializable {

   private String truststoreLocation;
    private String truststorePassword;
    private String protocol = "ssl";
    private String keystoreLocation= "None";
    private String keystorePassword = "None";
    private String keyPassword= "None";
    private String endpoint = "https";


}
