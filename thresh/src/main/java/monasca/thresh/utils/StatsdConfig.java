/*
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
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

package monasca.thresh.utils;

import java.io.Serializable;
import java.util.Map;

/*
 * Intended to deserialize the statsdConfig element in the
 * /etc/monasca/thresh-config.yml
 */
public class StatsdConfig implements Serializable {

  private static final long serialVersionUID = 3634080153227179376L;

  private String host;

  private Integer port;

  private String prefix;

  private Map<String, String> dimensions;

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(Map<String, String> dimensions) {
    this.dimensions = dimensions;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }
}
