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

import java.io.IOException;
import java.io.StringWriter;
import java.lang.Boolean;
import java.lang.String;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import monasca.common.streaming.storm.Logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.timgroup.statsd.NonBlockingUdpSender;
import com.timgroup.statsd.StatsDClientErrorHandler;

public class StatsdMetricConsumer implements IMetricsConsumer {

  public static final String STATSD_HOST = "metrics.statsd.host";
  public static final String STATSD_PORT = "metrics.statsd.port";
  public static final String STATSD_METRICMAP = "metrics.statsd.metricmap";
  public static final String STATSD_WHITELIST = "metrics.statsd.whitelist";
  public static final String STATSD_DIMENSIONS = "metrics.statsd.dimensions";
  public static final String STATSD_DEBUGMETRICS = "metrics.statsd.debugmetrics";

  private String topologyName;
  private String statsdHost = "localhost";
  private int statsdPort = 8125;
  private String monascaStatsdDimPrefix = "|#";
  private List<String> whiteList = new ArrayList<String>();
  private Map<String, String> metricMap = new HashMap<String, String>();
  private Boolean debugMetrics = false;


  String defaultDimensions = new StringBuilder().append(monascaStatsdDimPrefix)
      .append("{\"service\":\"monitoring\",\"component\":\"storm\"}")
      .toString();
  String statsdDimensions = defaultDimensions;

  /*
   * https://github.com/stackforge/monasca-agent#statsd
   *
   * Example metric produced from this code from Monasca statsd
   * filtering-bolt.sendqueue.read_pos:69
   * |c|#{"hostname":"localhost","service":"monitoring","component":"storm"}
   *
   * This is the Monasca specific string that adds the dimension element to
   * StatsD
   * |#{"hostname":"localhost","service":"monitoring","component":"storm"}
   *
   * To debug this code:
   * vi /usr/local/lib/python2.7/dist-packages/monasca_agent/statsd/udp.py
   * start():186 log.info('%s' % str(message))
   * sudo service monasca-agent restart
   * tail -f /var/log/monasca/agent/statsd.log
   * /vagrant/tests/smoke.py
   *
   * Note: You only know that "|#" is a delimeter by looking at the Monasca
   * Python Agent code since the Monasca StatsD server is a derivative of what
   * the general purpose StatsD implements and it is executed in the Monasca
   * Agent which was forked from DataDog. It extends UDP data by postfixing a
   * json struct describing the dimensions.
   */

  transient NonBlockingUdpSender udpclient;
  private transient StatsDClientErrorHandler handler;
  private transient Logger logger;

  @Override
  public void prepare(Map stormConf, Object registrationArgument,
      TopologyContext context, IErrorReporter errorReporter) {

    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));

    /* Sets up locals from the config STATSD_WHITELIST, STATSD_HOST ... */
    parseConfig(stormConf);

    /* Sets up local vars from config vars if present */
    if (registrationArgument instanceof Map) {
      parseConfig((Map<?, ?>) registrationArgument);
    }

    initClient();

    logger.info(
        "topologyName ({}), "
        + "clean(topologyName) ({})", new Object[] { topologyName,
            clean(topologyName) });
  }

  private void initClient() {
    try {
      handler = statsdErrorHandler;
      udpclient = new NonBlockingUdpSender(statsdHost, statsdPort,
          Charset.defaultCharset(), handler);
    }
    catch (IOException e) {
      /* NonBlockingUdpSender only throws an IOException */
      logger.error("{}", e);
    }
    catch (Exception e) {
      /* General purpose exception */
      logger.error("{}", e);
    }
  }

  StatsDClientErrorHandler statsdErrorHandler = new StatsDClientErrorHandler() {

    @Override
    public void handle(Exception e) {
      logger.error("Error with StatsD UDP client! {}", e);
    }
  };

  @SuppressWarnings("unchecked")
  void parseConfig(Map<?, ?> conf) {
    if (conf.containsKey(Config.TOPOLOGY_NAME)) {
      topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
    }

    if (conf.containsKey(STATSD_HOST)) {
      statsdHost = (String) conf.get(STATSD_HOST);
    }

    if (conf.containsKey(STATSD_PORT)) {
      statsdPort = ((Number) conf.get(STATSD_PORT)).intValue();
    }

    if (conf.containsKey(STATSD_DIMENSIONS)) {
      statsdDimensions = mapToJsonStr((Map<String, String>) conf
          .get(STATSD_DIMENSIONS));
      if (!isValidJSON(statsdDimensions)) {
        logger.error("Ignoring dimensions element invalid JSON ({})",
            new Object[] { statsdDimensions });
        // You get default dimensions
        statsdDimensions = monascaStatsdDimPrefix + defaultDimensions;
      }
      else {
        statsdDimensions = monascaStatsdDimPrefix + statsdDimensions;
      }
    }

    if (conf.containsKey(STATSD_WHITELIST)) {
      whiteList = (List<String>) conf.get(STATSD_WHITELIST);
    }

    if (conf.containsKey(STATSD_METRICMAP)) {
      metricMap = (Map<String, String>) conf.get(STATSD_METRICMAP);
    }

    if (conf.containsKey(STATSD_DEBUGMETRICS)) {
      debugMetrics = (Boolean) conf.get(STATSD_DEBUGMETRICS);
    }
  }

  private String mapToJsonStr(Map<String, String> inputMap) {
    String results = new String();
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();

    try {
      mapper.writeValue(sw, inputMap);
      results = sw.toString();
    }
    catch (JsonGenerationException e) {
      logger.error("{}", e);
    }
    catch (JsonMappingException e) {
      logger.error("{}", e);
    }
    catch (IOException e) {
      logger.error("{}", e);
    }

    return results;
  }

  private boolean isValidJSON(final String json) {
    boolean valid = false;
    try {
      final JsonParser parser = new ObjectMapper().getFactory().createParser(
          json);
      while (parser.nextToken() != null) {
      }
      valid = true;
    }
    catch (JsonParseException jpe) {
      valid = false;
    }
    catch (IOException ioe) {
      valid = false;
    }
    return valid;
  }

  String clean(String s) {
    /* storm metrics look pretty bad so cleanup is needed */
    return s.replace('.', '_').replace('/', '_').replace(':', '_')
        .replaceAll("__", "");
  }

  @Override
  public void handleDataPoints(TaskInfo taskInfo,
      Collection<DataPoint> dataPoints) {
    for (Metric metric : dataPointsToMetrics(taskInfo, dataPoints)) {
      reportUOM(metric.name, metric.value);
    }
  }

  public static class Metric {
    String name;
    Double value;

    public Metric(String name, Double value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Metric other = (Metric) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      }
      else if (!name.equals(other.name))
        return false;
      if (value != other.value)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Metric [name=" + name + ", value=" + value + "]";
    }
  }

  private List<Metric> dataPointsToMetrics(TaskInfo taskInfo,
      Collection<DataPoint> dataPoints) {
    List<Metric> res = new LinkedList<>();

    StringBuilder sb = new StringBuilder().append(
        clean(taskInfo.srcComponentId)).append(".");

    int hdrLength = sb.length();

    for (DataPoint p : dataPoints) {

      sb.delete(hdrLength, sb.length());
      sb.append(clean(p.name));

      logger.debug("Storm StatsD metric p.name ({}) p.value ({})",
          new Object[] { p.name, p.value });

      if (p.value instanceof Number) {
        res.add(new Metric(sb.toString(), ((Number) p.value).doubleValue()));
      }
      // There is a map of data points and it's not empty
      else if (p.value instanceof Map &&
          !(((Map<?, ?>) (p.value)).isEmpty())) {
        int hdrAndNameLength = sb.length();
        @SuppressWarnings("rawtypes")
        Map map = (Map) p.value;
        for (Object subName : map.keySet()) {
          Object subValue = map.get(subName);
          if (subValue instanceof Number) {
            sb.delete(hdrAndNameLength, sb.length());
            sb.append(".").append(clean(subName.toString()));

            res.add(new Metric(sb.toString(),
                ((Number) subValue).doubleValue()));
          }
        }
      }
    }
    return res;
  }

  /*
   * Since the Java client doesn't support the Monasca metric type we need to
   * build it with a raw UDP request
   */
  public void report(String s) {
    if (udpclient != null) {
      logger.debug("reporting: {}", s);
      udpclient.send(s);
    }
    else {
      /* Try to setup the UDP client since it was null */
      initClient();
    }
  }

  private void reportUOM(String s, Double number) {
    String metricName = null;
    StringBuilder results = new StringBuilder();
    Boolean published = false;

    if (whiteList.contains(s)) {

      if (!metricMap.isEmpty() && metricMap.containsKey(s)) {
        metricName = metricMap.get(s);
      }
      /* Send the unmapped uom as the same name storm calls it */
      else {
        metricName = s;
      }

      /* Make sure we don't send metric names that may be null or empty */
      if (metricName != null && !metricName.isEmpty()) {
        published = true;
      }
    }

    /*
     * To enable debug message, you also need to add an entry like this:
     *
     * <logger name="monasca.thresh" additivity="false">
     *   <level value="INFO" />
     *   <appender-ref ref="A1" />
     * </logger>
     *
     * Storm/Thresh logger config file:
     *   /opt/storm/apache-storm-0.9.5/logback/cluster.xml
     *
    */

    if (debugMetrics) {
      String mappedName = new String();

      if (!metricMap.isEmpty() && metricMap.containsKey(s)) {
        mappedName = metricMap.get(s);
      }
      else {
        mappedName = s;
      }

      logger.info(", RawMetricName, {}, MappedMetricName, {}, "
          + "val, {}, {}", new Object[]
          { s, mappedName, number,
              published == true ? "PUBLISHED" : "UNPUBLISHED"});
    }

    if (published) {
      results = results.append(metricName).append(":")
          .append(String.valueOf(number)).append("|c")
          .append(statsdDimensions);

      report(results.toString());
    }
  }

  @Override
  public void cleanup() {
    udpclient.stop();
  }
}
