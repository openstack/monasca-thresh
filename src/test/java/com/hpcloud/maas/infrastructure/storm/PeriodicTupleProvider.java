package com.hpcloud.maas.infrastructure.storm;

import java.util.List;

import com.hpcloud.maas.infrastructure.storm.TestSpout.TupleProvider;

/**
 * @author Jonathan Halterman
 */
public class PeriodicTupleProvider implements TupleProvider {
  @Override
  public List<Object> get() {
    return null;
  }
}
