package com.hpcloud.maas.infrastructure.persistence;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;

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
  public Alarm findById(String id) {
    return null;
  }
}
