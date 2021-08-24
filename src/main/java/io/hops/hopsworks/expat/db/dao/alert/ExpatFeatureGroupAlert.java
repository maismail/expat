/*
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
 */
package io.hops.hopsworks.expat.db.dao.alert;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;
import io.hops.hopsworks.persistence.entity.alertmanager.AlertType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class ExpatFeatureGroupAlert extends ExpatAbstractEntity<ExpatFeatureGroupAlert> {
  private Integer id;
  private String status;
  private AlertType alertType;
  private String severity;
  private Date created;
  private Integer featureGroup;
  private Integer receiver;
  
  public ExpatFeatureGroupAlert() {
  }
  
  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }
  
  public String getStatus() {
    return status;
  }
  
  public void setStatus(String status) {
    this.status = status;
  }
  
  public AlertType getAlertType() {
    return alertType;
  }
  
  public void setAlertType(AlertType alertType) {
    this.alertType = alertType;
  }
  
  public String getSeverity() {
    return severity;
  }
  
  public void setSeverity(String severity) {
    this.severity = severity;
  }
  
  public Date getCreated() {
    return created;
  }
  
  public void setCreated(Date created) {
    this.created = created;
  }
  
  public Integer getFeatureGroup() {
    return featureGroup;
  }
  
  public void setFeatureGroup(Integer featureGroup) {
    this.featureGroup = featureGroup;
  }
  
  public Integer getReceiver() {
    return receiver;
  }
  
  public void setReceiver(Integer receiver) {
    this.receiver = receiver;
  }
  
  @Override
  public ExpatFeatureGroupAlert getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.status = resultSet.getString("status");
    this.alertType = AlertType.valueOf(resultSet.getString("type"));
    this.severity = resultSet.getString("severity");
    this.created = resultSet.getDate("created");
    this.featureGroup = resultSet.getInt("feature_group_id");
    this.receiver = resultSet.getInt("receiver");
    return this;
  }
}
