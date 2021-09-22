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
 *
 */
package io.hops.hopsworks.expat.db.dao.util;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;
import io.hops.hopsworks.persistence.entity.util.VariablesVisibility;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExpatVariables extends ExpatAbstractEntity<ExpatVariables> {
  private String id;
  private String value;
  private VariablesVisibility visibility;
  private VariablesVisibility[] variablesVisibilityValues;
  
  public ExpatVariables() {
  }
  
  public ExpatVariables(String id, String value, VariablesVisibility visibility) {
    this.id = id;
    this.value = value;
    this.visibility = visibility;
  }
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public String getValue() {
    return value;
  }
  
  public void setValue(String value) {
    this.value = value;
  }
  
  public VariablesVisibility getVisibility() {
    return visibility;
  }
  
  public void setVisibility(VariablesVisibility visibility) {
    this.visibility = visibility;
  }
  
  @Override
  public ExpatVariables getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getString("id");
    this.value = resultSet.getString("value");
    if (variablesVisibilityValues == null)
      variablesVisibilityValues = VariablesVisibility.values();
    this.visibility = variablesVisibilityValues[resultSet.getInt("visibility")];
    return this;
  }
}
