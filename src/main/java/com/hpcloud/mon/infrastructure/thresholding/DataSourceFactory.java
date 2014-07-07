/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hpcloud.mon.infrastructure.thresholding;

import java.io.Serializable;
import java.util.Properties;

/**
 * This class replaces io.dropwizard.db.DataSourceFactory which currently can't be used with Storm
 * because it is not marked Serializable. This class could be deleted and replaced by that class
 * when and if io.dropwizard.db.DataSourceFactory is marked Serializable.
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
