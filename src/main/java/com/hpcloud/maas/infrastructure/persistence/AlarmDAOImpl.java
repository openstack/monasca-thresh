package com.hpcloud.maas.infrastructure.persistence;

import java.util.List;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;

import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.service.AlarmDAO;

/**
 * Alarm DAO implementation.
 * 
 * @author Jonathan Halterman
 */
public class AlarmDAOImpl implements AlarmDAO {
  private final DBI db;

  @Inject
  public AlarmDAOImpl(DBI db) {
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

  @Override
  public Alarm findByCompositeId(String compositeAlarmId) {
    return null;
  }
}
