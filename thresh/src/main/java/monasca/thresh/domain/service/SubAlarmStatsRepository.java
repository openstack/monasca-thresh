/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.thresh.domain.service;

import monasca.thresh.domain.model.SubAlarmStats;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * SubAlarmStats repository.
 */
public class SubAlarmStatsRepository {
  private final Map<String, SubAlarmStats> subAlarmStats = new HashMap<String, SubAlarmStats>();

  /**
   * Creates a new SubAlarmStats instance for the {@code subAlarm} and {@code viewEndTimestamp} and
   * adds it to the repository.
   */
  public void add(final String subAlarmId, SubAlarmStats newSubAlarmStats) {
    if (!subAlarmStats.containsKey(subAlarmId)) {
      subAlarmStats.put(subAlarmId, newSubAlarmStats);
    }
  }

  public Collection<SubAlarmStats> get() {
    return subAlarmStats.values();
  }

  public SubAlarmStats get(String subAlarmId) {
    return subAlarmStats.get(subAlarmId);
  }

  public boolean isEmpty() {
    return subAlarmStats.isEmpty();
  }

  public void remove(String subAlarmId) {
    subAlarmStats.remove(subAlarmId);
  }

  @Override
  public String toString() {
    return String.format("SubAlarmStatsRepository [subAlarmStats=%s]", subAlarmStats);
  }
}
