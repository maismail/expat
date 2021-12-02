/**
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
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

package io.hops.hopsworks.expat.migrations.kafka;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.avro.Schema;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InferenceSchemaV4Migration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(InferenceSchemaV4Migration.class);
  
  protected Connection connection;
  private boolean dryRun;
  
  // queries
  private final static String GET_PROJECT_IDS = "SELECT id FROM project";
  private final static String GET_SUBJECT_COMPATIBILITIES = "SELECT id FROM subjects_compatibility " +
    "WHERE subject = ?";
  private final static String UPDATE_SUBJECT_COMPATIBILITY = "UPDATE subjects_compatibility SET compatibility = ? " +
    "WHERE id = ?";
  private final static String INSERT_SUBJECT = "REPLACE INTO subjects " +
      "(subject, version, schema_id, project_id) VALUES" +
    " (?, ?, ?, ?)";
  private final static String INSERT_SCHEMA = "REPLACE INTO `schemas` " +
      "(`schema`, project_id) VALUES (?, ?)";
  private final static String GET_SCHEMA = "SELECT id FROM `schemas` WHERE `schema` = ? AND project_id = ?";
  private final static String DELETE_SUBJECT = "DELETE FROM subjects WHERE subject = ? AND version = ? AND project_id" +
    " = ?";
  private final static String DELETE_SCHEMA = "DELETE FROM `schemas` WHERE `schema` = ? AND project_id = ?";
  
  // kafka const
  private final static String SCHEMA_COMPATIBILITY_NONE = "NONE";
  private final static String INFERENCE_SCHEMA_NAME = "inferenceschema";
  private static final String INFERENCE_SCHEMA_VERSION_4 = "{\"fields\": [{\"name\": \"servingId\", \"type\": " +
    "\"int\"}, " + "{ \"name\": \"modelName\", \"type\": \"string\" }, {  \"name\": \"modelVersion\",  \"type\": " +
    "\"int\" }, " + "{  \"name\": \"requestTimestamp\",  \"type\": \"long\" }, {  \"name\": \"responseHttpCode\",  " +
    "\"type\": \"int\" }, {  \"name\": \"inferenceId\",  \"type\": \"string\" }, {  " + "\"name\": \"messageType\",  " +
    "\"type\": \"string\" }, { \"name\": \"payload\", \"type\": \"string\" } ]," + "  \"name\": \"inferencelog\",  " +
    "\"type\": \"record\" }";
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting inference schema v4 migration");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
  
    PreparedStatement getProjectIdsStmt = null;
    PreparedStatement getSubjectsCompatibilityStmt = null;
    PreparedStatement updateSubjectCompatibilityStmt = null;
    PreparedStatement insertSchemaStmt = null;
    PreparedStatement getSchemaStmt = null;
    PreparedStatement insertSubjectStmt = null;
    try {
      connection.setAutoCommit(false);
      
      // Update subject compatibility
      // -- get inferenceschema subject compatibilities
      getSubjectsCompatibilityStmt = connection.prepareStatement(GET_SUBJECT_COMPATIBILITIES);
      getSubjectsCompatibilityStmt.setString(1, INFERENCE_SCHEMA_NAME);
      ResultSet subjectResultSet = getSubjectsCompatibilityStmt.executeQuery();
      
      // -- update subject compatibilities to NONE
      updateSubjectCompatibilityStmt = connection.prepareStatement(UPDATE_SUBJECT_COMPATIBILITY);
      while (subjectResultSet.next()) {
        int id = subjectResultSet.getInt(1);
        updateSubjectCompatibilityStmt.setString(1, SCHEMA_COMPATIBILITY_NONE);
        updateSubjectCompatibilityStmt.setInt(2, id);
        updateSubjectCompatibilityStmt.addBatch();
      }
      if (dryRun) {
        LOGGER.info(updateSubjectCompatibilityStmt.toString());
      } else {
        updateSubjectCompatibilityStmt.executeBatch();
      }
      
      // Create schema and subject
      // -- per project
      getProjectIdsStmt = connection.prepareStatement(GET_PROJECT_IDS);
      ResultSet projectIdsResultSet = getProjectIdsStmt.executeQuery();
      String inferenceSchemaV4 = (new Schema.Parser().parse(INFERENCE_SCHEMA_VERSION_4)).toString();
      while(projectIdsResultSet.next()) {
        int projectId = projectIdsResultSet.getInt(1);
  
        // -- create schema
        insertSchemaStmt = connection.prepareStatement(INSERT_SCHEMA);
        insertSchemaStmt.setString(1, inferenceSchemaV4);
        insertSchemaStmt.setInt(2, projectId);
        if (dryRun) {
          LOGGER.info(insertSchemaStmt.toString());
        } else {
          insertSchemaStmt.execute();
        }
        getSchemaStmt = connection.prepareStatement(GET_SCHEMA);
        getSchemaStmt.setString(1, inferenceSchemaV4);
        getSchemaStmt.setInt(2, projectId);
        int schemaId = -1;
        if (dryRun) {
          LOGGER.info(getSchemaStmt.toString());
        } else {
          ResultSet schemaResultSet = getSchemaStmt.executeQuery();
          schemaResultSet.next(); // schema was inserted above
          schemaId = schemaResultSet.getInt(1);
        }
        
        // -- create subject
        insertSubjectStmt = connection.prepareStatement(INSERT_SUBJECT);
        insertSubjectStmt.setString(1, INFERENCE_SCHEMA_NAME);
        insertSubjectStmt.setInt(2, 4);
        insertSubjectStmt.setInt(3, schemaId);
        insertSubjectStmt.setInt(4, projectId);
        if (dryRun) {
          LOGGER.info(insertSubjectStmt.toString());
        } else {
          insertSubjectStmt.execute();
        }
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not migrate inferenceschema v4";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(getSubjectsCompatibilityStmt, updateSubjectCompatibilityStmt, getProjectIdsStmt,
        insertSchemaStmt, getSchemaStmt, insertSubjectStmt);
    }
    LOGGER.info("Finished inferenceschema v4 migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting inference schema v4 rollback");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
  
    PreparedStatement getProjectIdsStmt = null;
    PreparedStatement deleteSchemaStmt = null;
    PreparedStatement deleteSubjectStmt = null;
    try {
      connection.setAutoCommit(false);
      
      // Delete schema and subject
      // -- per project
      getProjectIdsStmt = connection.prepareStatement(GET_PROJECT_IDS);
      ResultSet projectIdsResultSet = getProjectIdsStmt.executeQuery();
      while(projectIdsResultSet.next()) {
        int projectId = projectIdsResultSet.getInt(1);
  
        // -- delete subject
        deleteSubjectStmt = connection.prepareStatement(DELETE_SUBJECT);
        deleteSubjectStmt.setString(1, INFERENCE_SCHEMA_NAME);
        deleteSubjectStmt.setInt(2, 4);
        deleteSubjectStmt.setInt(3, projectId);
        if (dryRun) {
          LOGGER.info(deleteSubjectStmt.toString());
        } else {
          deleteSubjectStmt.execute();
        }
  
        // -- delete schema
        deleteSchemaStmt = connection.prepareStatement(DELETE_SCHEMA);
        deleteSchemaStmt.setString(1, (new Schema.Parser().parse(INFERENCE_SCHEMA_VERSION_4)).toString());
        deleteSchemaStmt.setInt(2, projectId);
        if (dryRun) {
          LOGGER.info(deleteSchemaStmt.toString());
        } else {
          deleteSchemaStmt.execute();
        }
      }
    
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not rollback inferenceschema v4";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(getProjectIdsStmt, deleteSchemaStmt, deleteSubjectStmt);
    }
    LOGGER.info("Finished inferenceschema v4 rollback");
  }
  
  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
    Configuration conf = ConfigurationBuilder.getConfiguration();
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
  }
  
  private void closeConnections(PreparedStatement... stmts) {
    try {
      for (PreparedStatement stmt : stmts) {
        if (stmt != null) {
          stmt.close();
        }
      }
    } catch(SQLException ex) {
      //do nothing
    }
  }
}
