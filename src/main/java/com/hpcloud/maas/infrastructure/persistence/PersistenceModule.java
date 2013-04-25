package com.hpcloud.maas.infrastructure.persistence;

import javax.inject.Singleton;

import org.skife.jdbi.v2.DBI;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.persistence.DatabaseConfiguration;

/**
 * Configures persistence related types.
 * 
 * @author Jonathan Halterman
 */
public class PersistenceModule extends AbstractModule {
  private final DatabaseConfiguration dbConfig;

  public PersistenceModule(DatabaseConfiguration dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  protected void configure() {
    bind(AlarmDAO.class).to(AlarmDAOImpl.class);
    bind(SubAlarmDAO.class).to(SubAlarmDAOImpl.class);
  }

  @Provides
  @Singleton
  public DBI dbi() {
    return new DBI(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword());
  }
}
