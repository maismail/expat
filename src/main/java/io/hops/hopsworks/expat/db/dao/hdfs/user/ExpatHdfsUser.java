/*
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.db.dao.hdfs.user;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class ExpatHdfsUser extends ExpatAbstractEntity<ExpatHdfsUser> {
  private Integer id;
  private String name;
  
  public ExpatHdfsUser() {
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
  
  @Override
  public ExpatHdfsUser getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.name = resultSet.getString("name");
    return this;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpatHdfsUser hdfsUser = (ExpatHdfsUser) o;
    return Objects.equals(id, hdfsUser.id);
  }
}
