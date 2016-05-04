/*
 * (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package monasca.thresh;

import monasca.common.util.Injector;

import com.google.common.base.Preconditions;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.testng.annotations.Test;

@Test(groups = "integration")
public class TopologyTestCase {
  public static final String TEST_TOPOLOGY_NAME = "test-maas-alarming";
  protected static volatile LocalCluster cluster;

  protected void startTopology() throws Exception {
    if (cluster == null) {
      synchronized (TopologyTestCase.class) {
        if (cluster == null) {
          Preconditions.checkArgument(Injector.isBound(Config.class),
              "You must bind a storm config");
          Preconditions.checkArgument(Injector.isBound(StormTopology.class),
              "You must bind a storm topology");

          cluster = new LocalCluster();
          cluster.submitTopology(TEST_TOPOLOGY_NAME, Injector.getInstance(Config.class),
              Injector.getInstance(StormTopology.class));
        }
      }
    }
  }

  protected static void stopTopology() {
    if (cluster != null) {
      cluster.killTopology(TEST_TOPOLOGY_NAME);
      cluster.shutdown();
      cluster = null;
    }
  }
}
