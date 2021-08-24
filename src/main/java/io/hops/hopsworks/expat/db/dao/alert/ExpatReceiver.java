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

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExpatReceiver extends ExpatAbstractEntity<ExpatReceiver> {
  private Integer id;
  private String name;
  private byte[] config;
  
  public ExpatReceiver() {
  }
  
  public ExpatReceiver(String name) {
    this.name = name;
  }
  
  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public byte[] getConfig() {
    return config;
  }
  
  public void setConfig(byte[] config) {
    this.config = config;
  }
  
  @Override
  public ExpatReceiver getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.name = resultSet.getString("name");
    this.config = resultSet.getBytes("config");
    return this;
  }
}
