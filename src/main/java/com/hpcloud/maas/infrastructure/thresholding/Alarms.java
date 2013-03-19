package com.hpcloud.maas.infrastructure.thresholding;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hpcloud.maas.domain.model.Alarm;

public final class Alarms {
  private Alarms() {
  }

  public static String alarmIdFor(String metric) {
    // self.key_dict[memory_key] = self.cass.get_metric_key_or_create(
    // message['project_id'], message['namespace'],
    // message['metric_name'], message['dimensions'],
    // message['unit']
    // )
    //
    return null;
  }

  public static Alarm alarmFor(String metric) {
    return null;
  }

  LoadingCache<String, Alarm> graphs = CacheBuilder.newBuilder().build(
      new CacheLoader<String, Alarm>() {
        public Alarm load(String key) {
          // return createExpensiveGraph(key);
          return null;
        }
      });
}
