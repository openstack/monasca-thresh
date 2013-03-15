package com.hpcloud.maas.infrastructure.persistence;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.persistence.BeanMapper;
import com.hpcloud.persistence.SqlQueries;

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
  public List<Alarm> find() {
    Handle h = db.open();

    try {
      List<Alarm> alarms = h.createQuery("select * from alarm")
          .map(new BeanMapper<Alarm>(Alarm.class))
          .list();

      // Hydrate dimensions
      for (Alarm alarm : alarms)
        alarm.setDimensions(findDimensionById(h, alarm.getId()));

      return alarms;
    } finally {
      h.close();
    }
  }

  private Map<String, String> findDimensionById(Handle handle, String alarmId) {
    return SqlQueries.keyValuesFor(handle,
        "select dimension_name, value from alarm_dimension where alarm_id = ?", alarmId);
  }
}
