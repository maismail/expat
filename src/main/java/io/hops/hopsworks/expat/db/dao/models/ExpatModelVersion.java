/*
 * This file is part of Expat
 * Copyright (C) 2023, Hopsworks AB. All rights reserved
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
package io.hops.hopsworks.expat.db.dao.models;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class ExpatModelVersion extends ExpatAbstractEntity<ExpatModelVersion> {

  private Integer version;

  private Integer modelId;

  private Integer userId;

  private String userFullName;

  private Date created;

  private String description;

  private String metrics;

  private String program;

  private String framework;

  private String environment;

  private String experimentId;

  private String experimentProjectName;

  public ExpatModelVersion() {
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public Integer getModelId() {
    return modelId;
  }

  public void setModelId(Integer modelId) {
    this.modelId = modelId;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public String getUserFullName() {
    return userFullName;
  }

  public void setUserFullName(String userFullName) {
    this.userFullName = userFullName;
  }

  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getMetrics() {
    return metrics;
  }

  public void setMetrics(String metrics) {
    this.metrics = metrics;
  }

  public String getProgram() {
    return program;
  }

  public void setProgram(String program) {
    this.program = program;
  }

  public String getFramework() {
    return framework;
  }

  public void setFramework(String framework) {
    this.framework = framework;
  }

  public String getEnvironment() {
    return environment;
  }

  public void setEnvironment(String environment) {
    this.environment = environment;
  }

  public String getExperimentId() {
    return experimentId;
  }

  public void setExperimentId(String experimentId) {
    this.experimentId = experimentId;
  }

  public String getExperimentProjectName() {
    return experimentProjectName;
  }

  public void setExperimentProjectName(String experimentProjectName) {
    this.experimentProjectName = experimentProjectName;
  }

  @Override
  public ExpatModelVersion getEntity(ResultSet resultSet) throws SQLException {
    this.setVersion(resultSet.getInt("version"));
    this.setModelId(resultSet.getInt("model_id"));
    this.setUserId(resultSet.getInt("user_id"));
    this.setCreated(resultSet.getDate("created"));
    this.setDescription(resultSet.getString("description"));
    this.setMetrics(resultSet.getString("metrics"));
    this.setProgram(resultSet.getString("program"));
    this.setFramework(resultSet.getString("framework"));
    this.setEnvironment(resultSet.getString("environment"));
    this.setExperimentId(resultSet.getString("experiment_id"));
    this.setExperimentProjectName(resultSet.getString("experiment_project_name"));
    return this;
  }
}
