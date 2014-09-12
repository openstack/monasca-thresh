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

package monasca.thresh.infrastructure.persistence;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.thresholding.DataSourceFactory;

import org.skife.jdbi.v2.DBI;

import javax.inject.Singleton;

/**
 * Configures persistence related types.
 */
public class PersistenceModule extends AbstractModule {
  private final DataSourceFactory dbConfig;

  public PersistenceModule(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  protected void configure() {
    bind(AlarmDAO.class).to(AlarmDAOImpl.class).in(Scopes.SINGLETON);
    bind(AlarmDefinitionDAO.class).to(AlarmDefinitionDAOImpl.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Singleton
  public DBI dbi() throws Exception {
    Class.forName(dbConfig.getDriverClass());
    return new DBI(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword());
  }
}
