/*
 * This file is part of Expat
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class JupyterInConda implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(JupyterInConda.class);

  private String condaDir = null;
  private String condaUser = null;
  private String expatPath = null;

  private int hdfscontentsId = -1;
  private int sparkmagicId = -1;
  private int jupyterId = -1;

  @Override
  public void migrate() throws MigrationException {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    PreparedStatement projectDepsUpdate = null;

    try {
      setup();

      setupDepTables();
      if (hdfscontentsId == -1) {
        // New installation, return.
        return;
      }
      dbConn = DbConnectionFactory.getConnection();

      projectDepsUpdate = dbConn.prepareStatement("REPLACE INTO project_pythondeps VALUE (?, ?)");

      stmt = dbConn.createStatement();
      resultSet = stmt.executeQuery("SELECT id, projectname FROM project WHERE conda_env = 1");
      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        int projectId = resultSet.getInt("id");
        ProcessResult processResult = null;

        LOGGER.info("Installing jupyter & deps for project: " + projectName);

        try {
          ProcessDescriptor jupyterInstallProc = new ProcessDescriptor.Builder()
              .addCommand(expatPath + "/bin/jupyter_migrate.sh")
              .addCommand("install")
              .addCommand(projectName)
              .addCommand(condaDir)
              .addCommand(condaUser)
              .ignoreOutErrStreams(false)
              .setWaitTimeout(5,  TimeUnit.MINUTES)
              .build();

          processResult = ProcessExecutor.getExecutor().execute(jupyterInstallProc);
          if (processResult.getExitCode() == 0) {
            updateProjectPythonDeps(projectDepsUpdate, projectId);
          } else {
            LOGGER.error("Failed to install jupyter for project: " + projectName +
                " " + processResult.getStdout());
          }
        } catch (IOException e) {
          // Keep going
          LOGGER.error("Failed to install jupyter for project: " + projectName +
              " " + e.getMessage());
        }
      }
    } catch (SQLException | ConfigurationException e) {
      throw new MigrationException("Cannot fetch the list of projects from the database", e);
    } finally {
      try {
        if (dbConn != null) {
          dbConn.close();
        }
        if (stmt != null) {
          stmt.close();
        }
        if (projectDepsUpdate != null) {
          projectDepsUpdate.close();
        }
        if (resultSet != null) {
          resultSet.close();
        }
      } catch (SQLException e) { }
    }
  }

  @Override
  public void rollback() throws RollbackException {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    PreparedStatement projectDepsUpdate = null;

    try {
      setup();
      fetchLibIds();

      dbConn = DbConnectionFactory.getConnection();

      projectDepsUpdate = dbConn.prepareStatement("DELETE FROM project_pythondeps " +
          "WHERE project_id = ? AND dep_id = ?");

      stmt = dbConn.createStatement();
      resultSet = stmt.executeQuery("SELECT id, projectname FROM project WHERE conda_env = 1");

      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        int projectId = resultSet.getInt("id");

        LOGGER.info("Removing jupyter & deps for project: " + projectName);
        try {
          ProcessDescriptor jupyterInstallProc = new ProcessDescriptor.Builder()
              .addCommand(expatPath + "/bin/jupyter_migrate.sh")
              .addCommand("remove")
              .addCommand(projectName)
              .addCommand(condaDir)
              .addCommand(condaUser)
              .ignoreOutErrStreams(true)
              .build();

          ProcessExecutor.getExecutor().execute(jupyterInstallProc);

          updateProjectPythonDeps(projectDepsUpdate, projectId);
        } catch (IOException e) {
          // Keep going
          LOGGER.error("Failed to install jupyter for project: " + projectName, e);
        }
      }
    } catch (SQLException | ConfigurationException e) {
      throw new RollbackException("Cannot fetch the list of projects from the database", e);
    } finally {
      try {
        if (dbConn != null) {
          dbConn.close();
        }
        if (stmt != null) {
          stmt.close();
        }
        if (projectDepsUpdate != null) {
          projectDepsUpdate.close();
        }
        if (resultSet != null) {
          resultSet.close();
        }
      } catch (SQLException e) { }
    }
  }

  private void setupDepTables() throws SQLException, ConfigurationException {
    Connection dbConn = DbConnectionFactory.getConnection();
    Statement stmt = dbConn.createStatement();
    ResultSet resultSet = null;

    try {

      int PyPiID = -1;
      resultSet = stmt.executeQuery("SELECT id FROM anaconda_repo WHERE url='PyPi'");
      if (resultSet.next()) {
        PyPiID = resultSet.getInt("id");
      } else {
        // anaconda_repo didn't return any entry for the PyPi repo, most likely it's a fresh installation.
        return;
      }

      // Insert Python dependencies rows
      stmt.executeUpdate("REPLACE INTO python_dep(dependency, version, repo_id, " +
          "status, preinstalled, install_type, machine_type) VALUES " +
          "('hdfscontents', '0.7', " + PyPiID + ", 1, 1, 2, 0);");
      stmt.executeUpdate("REPLACE INTO python_dep(dependency, version, repo_id, " +
          "status, preinstalled, install_type, machine_type) VALUES " +
          "('sparkmagic', '0.12.5', " + PyPiID + ", 1, 1, 2, 0);");
      stmt.executeUpdate("REPLACE INTO python_dep(dependency, version, repo_id, " +
          "status, preinstalled, install_type, machine_type) VALUES " +
          "('jupyter', '1.0.0', " + PyPiID + ", 1, 1, 2, 0);");
    } finally {
      try {
        dbConn.close();
        if (stmt != null) {
          stmt.close();
        }
        if (resultSet != null) {
          resultSet.close();
        }
      } catch (SQLException e) { }
    }

    fetchLibIds();
  }

  private void fetchLibIds() throws SQLException, ConfigurationException {
    Connection dbConn = DbConnectionFactory.getConnection();
    Statement stmt = dbConn.createStatement();
    ResultSet resultSet = null;
    try {
      resultSet = stmt.executeQuery("SELECT id FROM python_dep WHERE dependency='hdfscontents'");
      if (resultSet.next()) {
        hdfscontentsId = resultSet.getInt("id");
      }
      resultSet.close();

      resultSet = stmt.executeQuery("SELECT id FROM python_dep WHERE dependency='sparkmagic'");
      if (resultSet.next()) {
        sparkmagicId = resultSet.getInt("id");
      }
      resultSet.close();

      resultSet = stmt.executeQuery("SELECT id FROM python_dep WHERE dependency='jupyter'");
      if (resultSet.next()) {
        jupyterId = resultSet.getInt("id");
      }
      resultSet.close();
    } finally {
      try {
        dbConn.close();
        if (stmt != null) {
          stmt.close();
        }
        if (resultSet != null) {
          resultSet.close();
        }
      } catch (SQLException e) { }
    }
  }

  private void updateProjectPythonDeps(PreparedStatement stmt, int projectId) throws SQLException {
    stmt.setInt(1, projectId);
    stmt.setInt(2, hdfscontentsId);
    stmt.executeUpdate();

    stmt.setInt(2, sparkmagicId);
    stmt.executeUpdate();

    stmt.setInt(2, jupyterId);
    stmt.executeUpdate();
  }

  private void setup() throws ConfigurationException {
    Configuration config = ConfigurationBuilder.getConfiguration();
    condaDir = config.getString(ExpatConf.CONDA_DIR);
    condaUser = config.getString(ExpatConf.CONDA_USER);
    expatPath = config.getString(ExpatConf.EXPAT_PATH);
  }
}
