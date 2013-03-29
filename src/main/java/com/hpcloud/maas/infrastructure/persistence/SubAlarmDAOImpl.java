package com.hpcloud.maas.infrastructure.persistence;

import java.util.List;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;

import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.SubAlarmDAO;

/**
 * Alarm DAO implementation.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarmDAOImpl implements SubAlarmDAO {
  private final DBI db;

  @Inject
  public SubAlarmDAOImpl(DBI db) {
    this.db = db;
  }

  @Override
  public List<SubAlarm> find(MetricDefinition metricDefinition) {
    // Handle h = db.open();
    //
    // try {
    // List<Alarm> alarms = h.createQuery("select * from alarm where tenant_id = :tenantId")
    // .bind("tenantId", tenantId)
    // .map(new BeanMapper<Alarm>(Alarm.class))
    // .list();
    //
    // for (Alarm alarm : alarms)
    // alarm.setMetricDefinition(metricDefinition);
    //
    // return alarms;
    // } finally {
    // h.close();
    // }
    return null;
  }
}
