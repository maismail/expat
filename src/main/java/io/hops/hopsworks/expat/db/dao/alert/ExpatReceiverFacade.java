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

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

public class ExpatReceiverFacade extends ExpatAbstractFacade<ExpatReceiver> {
  private static final String GET_ALL_RECEIVERS = "SELECT * FROM alert_receiver";
  private final static String ADD_RECEIVERS = "INSERT INTO alert_receiver (name, config) VALUES (?, ?)";
  private Connection connection;
  
  protected ExpatReceiverFacade(Class<ExpatReceiver> entityClass) throws ConfigurationException, SQLException {
    super(entityClass);
    this.connection = DbConnectionFactory.getConnection();
  }
  
  public ExpatReceiverFacade(Class<ExpatReceiver> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return GET_ALL_RECEIVERS;
  }
  
  @Override
  public String findByIdQuery() {
    return GET_ALL_RECEIVERS + " WHERE id = ?";
  }
  
  public ExpatReceiver find(Integer id) throws IllegalAccessException, SQLException, InstantiationException {
    return this.findById(id, JDBCType.INTEGER);
  }
  
  public void addReceiver(String name, byte[] config) throws SQLException {
    this.update(ADD_RECEIVERS, new Object[]{name, config}, new JDBCType[]{JDBCType.VARCHAR, JDBCType.BLOB});
  }
  
  public ExpatReceiver findByName(String name) throws SQLException, IllegalAccessException, InstantiationException {
    List<ExpatReceiver> resultList = this.findByQuery("SELECT * FROM alert_receiver WHERE name = ?", new Object[]{name},
        new JDBCType[]{JDBCType.VARCHAR});
    if (resultList.isEmpty()) {
      return null;
    }
    if (resultList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return resultList.get(0);
  }
}
