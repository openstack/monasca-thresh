package com.hpcloud.maas.infrastructure.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.hpcloud.maas.common.model.metric.CollectdMetrics;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.service.MetricDefinitionDAO;

/**
 * MetricDefinition DAO implementation.
 * 
 * @author Jonathan Halterman
 */
public class MetricDefinitionDAOImpl implements MetricDefinitionDAO {
  private static final String METRIC_DEF_SQL = "select sa.namespace, sad.dimensions from sub_alarm as sa "
      + "left join (select sub_alarm_id, group_concat(dimension_name, '=', value) as dimensions from sub_alarm_dimension group by sub_alarm_id) as sad on sa.id = sad.sub_alarm_id";

  private final DBI db;

  @Inject
  public MetricDefinitionDAOImpl(DBI db) {
    this.db = db;
  }

  @Override
  public List<MetricDefinition> findForAlarms() {
    Handle h = db.open();

    try {
      List<Map<String, Object>> rows = h.createQuery(METRIC_DEF_SQL).list();

      List<MetricDefinition> metricDefs = new ArrayList<MetricDefinition>(rows.size());
      for (Map<String, Object> row : rows) {
        String namespace = (String) row.get("namespace");
        String dimensionSet = (String) row.get("dimensions");
        Map<String, String> dimensions = null;

        if (dimensionSet != null) {
          for (String kvStr : dimensionSet.split(",")) {
            String[] kv = kvStr.split("=");
            // TODO Remove second conditional in the future
            if (kv.length > 1 && CollectdMetrics.isSupportedDimension(namespace, kv[0])) {
              if (dimensions == null)
                dimensions = new HashMap<String, String>();
              dimensions.put(kv[0], kv[1]);
            }
          }
        }

        metricDefs.add(new MetricDefinition(namespace, dimensions));
      }

      return metricDefs;
    } finally {
      h.close();
    }
  }
}
