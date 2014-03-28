package com.hpcloud.mon.infrastructure.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;

/**
 * MetricDefinition DAO implementation.
 * 
 * @author Jonathan Halterman
 */
public class MetricDefinitionDAOImpl implements MetricDefinitionDAO {
  private static final String METRIC_DEF_SQL = "select sa.metric_name, sad.dimensions from alarm as a, sub_alarm as sa "
      + "left join (select sub_alarm_id, group_concat(dimension_name, '=', value) as dimensions from sub_alarm_dimension group by sub_alarm_id) as sad on sa.id = sad.sub_alarm_id "
      + "where a.id = sa.alarm_id and a.deleted_at is null";

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
        String metric_name = (String) row.get("metric_name");
        String dimensionSet = (String) row.get("dimensions");
        Map<String, String> dimensions = null;

        if (dimensionSet != null) {
          for (String kvStr : dimensionSet.split(",")) {
            String[] kv = kvStr.split("=");
            if (kv.length > 1) {
              if (dimensions == null)
                dimensions = new HashMap<String, String>();
              dimensions.put(kv[0], kv[1]);
            }
          }
        }

        metricDefs.add(new MetricDefinition(metric_name, dimensions));
      }

      return metricDefs;
    } finally {
      h.close();
    }
  }
}
