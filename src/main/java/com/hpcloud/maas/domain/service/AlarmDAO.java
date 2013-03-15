package com.hpcloud.maas.domain.service;

import java.util.List;

import com.hpcloud.maas.domain.model.Alarm;

/**
 * Alarm DAO.
 * 
 * @author Jonathan Halterman
 */
public interface AlarmDAO {
  /** Finds and returns all subscriptions. */
  List<Alarm> find();
}
