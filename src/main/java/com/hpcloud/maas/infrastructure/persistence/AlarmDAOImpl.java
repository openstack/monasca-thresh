package com.hpcloud.maas.infrastructure.persistence;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.persistence.BeanMapper;

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
  public Alarm findById(String id) {
    Handle h = db.open();

    try {
      Alarm alarm = h.createQuery("select * from alarm where id = :id")
          .bind("id", id)
          .map(new BeanMapper<Alarm>(Alarm.class))
          .first();

      alarm.setSubAlarms(SubAlarmDAOImpl.subAlarmsForRows(
          h,
          h.createQuery("select * from sub_alarm where alarm_id = :alarmId")
              .bind("alarmId", alarm.getId())
              .list()));

      return alarm;
    } finally {
      h.close();
    }
  }

  @Override
  public void updateState(String id, AlarmState state) {
    Handle h = db.open();

    try {
      h.createStatement("update alarm set state = :state where id = :id")
          .bind("id", id)
          .bind("state", state.toString())
          .execute();
    } finally {
      h.close();
    }
  }
}
