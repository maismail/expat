package io.hops.hopsworks.expat.migrations.conda;

import io.hops.hopsworks.common.util.ProcessDescriptor;
import io.hops.hopsworks.common.util.ProcessResult;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.executor.ProcessExecutor;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class DockerConda implements MigrateStep {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DockerConda.class);
  
  private String condaDir = null;
  private String condaUser = null;
  private String expatPath = null;
  private String hadoopHome = null;
  private String hopsClientUser = null;
  
  private void setup() throws ConfigurationException {
    Configuration config = ConfigurationBuilder.getConfiguration();
    condaDir = config.getString(ExpatConf.CONDA_DIR) + "/anaconda";
    condaUser = config.getString(ExpatConf.CONDA_USER);
    expatPath = config.getString(ExpatConf.EXPAT_PATH);
    hopsClientUser = config.getString(ExpatConf.HOPS_CLIENT_USER);
    hadoopHome = System.getenv("HADOOP_HOME");
  }
  
  @Override
  public void migrate() throws MigrationException {
    //Get all conda enabled projects
    try (Connection connection = DbConnectionFactory.getConnection();
         Statement stmt = connection.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT projectname,users.username FROM project " +
                                                    "JOIN users ON project.username=users.email;")) {
      setup();
      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        String username = resultSet.getString("username");
        //Get username of owner and construct project_user username
        String projectUser = projectName + "__" + username;
        
        try {
          ProcessDescriptor convaEnvMigrateProc = new ProcessDescriptor.Builder()
            .addCommand(expatPath + "/bin/conda_env_migrate.sh")
            .addCommand(projectName)
            .addCommand(condaDir)
            .addCommand(condaUser)
            .addCommand(projectUser)
            .addCommand(hopsClientUser)
            .addCommand(hadoopHome)
            .ignoreOutErrStreams(false)
            .setWaitTimeout(2, TimeUnit.MINUTES)
            .build();
          
          ProcessResult processResult = ProcessExecutor.getExecutor().execute(convaEnvMigrateProc);
          if (processResult.getExitCode() == 0) {
            LOGGER.info("Successfully exported Python env for project: " + projectName);
          } else if (processResult.getExitCode() == 2) {
            LOGGER.info("Project: " + projectName + " is using the default Python environment");
          } else {
            LOGGER.error("Failed to export Python env for project: " + projectName +
              " " + processResult.getStderr());
          }
        } catch (IOException e) {
          // Keep going
          LOGGER.error("Failed to export conda env project: " + projectName + " " + e.getMessage());
        }
      }
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      throw new MigrationException("Error in migration step " + DockerConda.class.getSimpleName(), ex);
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
  
  }
}
