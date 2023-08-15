package io.hops.hopsworks.expat.migrations.featurestore.featureview;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatInodeController;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class FeatureStoreMigration implements MigrateStep {

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureStoreMigration.class);

  protected Connection connection;
  protected DistributedFileSystemOps dfso = null;
  protected boolean dryRun;
  protected String hopsUser;
  protected ExpatInodeController inodeController;
  protected Configuration conf;

  public FeatureStoreMigration() {
  }

  public abstract void runRollback() throws RollbackException;

  public abstract void runMigration() throws MigrationException, SQLException;

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting migration of " + super.getClass().getName());

    try {
      setup();
      runMigration();
      close();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new MigrationException(errorMsg, ex);
    }
    LOGGER.info("Finished migration of " + super.getClass().getName());
  }

  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting rollback of " + super.getClass().getName());
    try {
      setup();
      runRollback();
      close();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new RollbackException(errorMsg, ex);
    }
    LOGGER.info("Finished rollback of " + super.toString());
  }


  protected void setup()
      throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();

    conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    inodeController = new ExpatInodeController(this.connection);
  }

  protected void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOGGER.error("failed to close jdbc connection", ex);
      }
    }
    if (dfso != null) {
      dfso.close();
    }
  }
}
