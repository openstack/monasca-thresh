package com.hpcloud.mon.infrastructure.thresholding;

import java.io.Serializable;
import java.util.Properties;

/**
 * This class replaces io.dropwizard.db.DataSourceFactory which currently can't be used
 * with Storm because it is not marked Serializable. This class should be deleted and replaced
 * by that class when io.dropwizard.db.DataSourceFactory is marked Serializable.
 *
 * @author craigbr
 *
 */
public class DataSourceFactory implements Serializable {

    private static final long serialVersionUID = -1903552028062110222L;

    private String user;

    private String password;

    private String url;

    private String driverClass;

    private Properties properties;

    private String maxWaitForConnection;

    private String validationQuery;

    private String minSize;

    private String maxSize;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getMaxWaitForConnection() {
        return maxWaitForConnection;
    }

    public void setMaxWaitForConnection(String maxWaitForConnection) {
        this.maxWaitForConnection = maxWaitForConnection;
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    public String getMinSize() {
        return minSize;
    }

    public void setMinSize(String minSize) {
        this.minSize = minSize;
    }

    public String getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(String maxSize) {
        this.maxSize = maxSize;
    }
}
