package com.hpcloud.mon.infrastructure.persistence;

import io.dropwizard.db.DataSourceFactory;

import javax.inject.Singleton;

import org.skife.jdbi.v2.DBI;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;

/**
 * Configures persistence related types.
 * 
 * @author Jonathan Halterman
 */
public class PersistenceModule extends AbstractModule {
  private final DataSourceFactory dbConfig;

  public PersistenceModule(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  protected void configure() {
    bind(AlarmDAO.class).to(AlarmDAOImpl.class).in(Scopes.SINGLETON);
    bind(SubAlarmDAO.class).to(SubAlarmDAOImpl.class).in(Scopes.SINGLETON);
    bind(MetricDefinitionDAO.class).to(MetricDefinitionDAOImpl.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Singleton
  public DBI dbi() throws Exception {
    Class.forName(dbConfig.getDriverClass());
    return new DBI(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword());
  }
}
